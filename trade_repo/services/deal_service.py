import logging
import os
from datetime import date, datetime, time
from pathlib import Path

import requests

from app.trade_repo.models.deal_model import Deal
from app.trade_repo.models.event_model import EventQueue
from app.trade_repo.repos.message_repository import MessageRepository
from app.trade_repo.repos.trade_repository import TradeRepository
from app.trade_repo.repos.audit_repository import AuditRepository
from app.trade_repo.services.validation_service import ValidationService
from app.trade_repo.services.xml_parser_service import XMLParserService
from app.trade_repo.services.status_service import StatusTransitionService

logger = logging.getLogger(__name__)
XML_FOLDER = Path.cwd() / "xml"
XML_FOLDER.mkdir(parents=True, exist_ok=True)


def save_deal_xml(deal_id, xml_body):
    file_path = XML_FOLDER / f"{deal_id}.xml"
    logger.info(f"Saving booked deal XML to {file_path}")
    file_path.write_text(xml_body, encoding="utf-8")
    return str(file_path)


def parse_date(value):
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


def parse_float(value):
    if isinstance(value, str):
        return float(value)
    return value


class DealService:
    @staticmethod
    def _ssix_base_url():
        return os.getenv("SSI_API_BASE_URL", "http://127.0.0.1:8000/ssi")

    @staticmethod
    def _message_hub_base_url():
        return os.getenv("MESSAGE_HUB_API_BASE_URL", "http://127.0.0.1:8000/message-hub")

    @staticmethod
    def _request_json(method, url, payload=None, params=None):
        response = requests.request(method, url, json=payload, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _build_ssi_request(deal):
        trade_date = parse_date(deal.trade_date)
        return {
            "trade_reference": deal.deal_id,
            "buy_counterparty": deal.counterparty_id,
            "sell_counterparty": deal.counterparty_id,
            "buy_currency": deal.sold_ccy,
            "sell_currency": deal.bought_ccy,
            "trade_date": trade_date.isoformat() if trade_date else None,
            "amount": float(deal.sold_amount or 0),
        }

    @staticmethod
    def _build_message_hub_request(deal):
        trade_date = parse_date(deal.trade_date)
        value_date = parse_date(deal.value_date)
        if isinstance(trade_date, date):
            trade_date = datetime.combine(trade_date, time.min)
        if isinstance(value_date, date):
            value_date = datetime.combine(value_date, time.min)

        return {
            "deal_id": deal.deal_id,
            "trade_date": trade_date.isoformat() if trade_date else None,
            "value_date": value_date.isoformat() if value_date else None,
            "operation_type": "NEWT",
            "cagm_bic_code": os.getenv("CAGM_BIC_CODE", deal.counterparty_id or "CAGMXXXX"),
            "cagm_buy_amount": float(deal.sold_amount or 0),
            "cagm_buy_currency": deal.sold_ccy,
            "counterparty_bic_code": deal.counterparty_id or "COUNTERPTY",
            "ctr_sell_amount": float(deal.bought_amount or 0),
            "ctr_sell_currency": deal.bought_ccy,
            "spot_rate": float(deal.spot_rate or 0),
        }

    @staticmethod
    def _resolve_ssi_via_api(deal):
        url = f"{DealService._ssix_base_url().rstrip('/')}/api/v1/resolve-ssi"
        payload = DealService._build_ssi_request(deal)
        return DealService._request_json("POST", url, payload=payload)

    @staticmethod
    def _generate_fxtr014_via_api(deal):
        url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/generate"
        payload = DealService._build_message_hub_request(deal)
        return DealService._request_json("POST", url, payload=payload)

    @staticmethod
    def _enrich_fxtr014_via_api(fxtr014_id, buy_ssi_id, sell_ssi_id):
        url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/{fxtr014_id}/enrich"
        params = {
            "cagm_ssi_id": buy_ssi_id,
            "cagm_ssi_version": 1,
            "counterparty_ssi_id": sell_ssi_id,
            "counterparty_ssi_version": 1,
        }
        return DealService._request_json("POST", url, params=params)

    @staticmethod
    def _generate_mt300_via_api(fxtr014_id):
        url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/{fxtr014_id}/generate-mt300"
        return DealService._request_json("POST", url)

    @staticmethod
    def _mt300_ack_via_api(fxtr014_id):
        url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/{fxtr014_id}/mt300-ack"
        return DealService._request_json("POST", url)

    @staticmethod
    def _orchestrated_request(method, url, deal_id, endpoint_name, source, json_payload=None, params=None):
        import json
        import requests
        
        req_str = f"Method: {method}\nURL: {url}"
        if json_payload:
            req_str += f"\nBody:\n{json.dumps(json_payload, indent=2)}"
        if params:
            req_str += f"\nParams:\n{json.dumps(params, indent=2)}"
            
        try:
            response = requests.request(method, url, json=json_payload, params=params, timeout=15)
            res_status = response.status_code
            try:
                res_body = response.json()
                res_str = f"Status: {res_status}\nBody:\n{json.dumps(res_body, indent=2)}"
            except Exception:
                res_body = response.text
                res_str = f"Status: {res_status}\nBody:\n{res_body}"
                
            DealService._log_api_trace(deal_id, endpoint_name, source, req_str, res_str)
            response.raise_for_status()
            return res_body
        except Exception as exc:
            res_str = f"Error: {str(exc)}"
            DealService._log_api_trace(deal_id, endpoint_name, source, req_str, res_str)
            raise

    @staticmethod
    def _update_pipeline_status(deal_id: str, status: str, error: str = None, ssi_type: str = None, buy_id: str = None, sell_id: str = None, payload: str = None):
        from app.trade_repo.core.db import MessageSessionLocal
        from app.message_hub.repos.pipeline_repository import PipelineRepository
        
        db = MessageSessionLocal()
        try:
            repo = PipelineRepository(db)
            repo.update_status(
                deal_id=deal_id,
                status=status,
                last_error=error,
                ssi_type=ssi_type,
                ssi_version_buy_id=buy_id,
                ssi_version_sell_id=sell_id,
                status_payload=payload
            )
        except Exception:
            logger.exception("Failed to update message hub pipeline status")
        finally:
            db.close()

    @staticmethod
    def _log_api_trace(deal_id: str, endpoint: str, source: str, request_data: str, response_data: str):
        from app.trade_repo.core.db import MessageSessionLocal
        from app.message_hub.models.pipeline_model import StatusMonitor
        
        db = MessageSessionLocal()
        try:
            payload_str = f"Request Details:\n{request_data}\n\nResponse Details:\n{response_data}"
            monitor = StatusMonitor(
                deal_id=deal_id,
                status=endpoint,
                source=source,
                payload=payload_str,
                created_at=datetime.utcnow()
            )
            db.add(monitor)
            db.commit()
            db.refresh(monitor)
        except Exception as e:
            logger.exception("Failed to write API trace to status_monitor database: %s", str(e))
        finally:
            db.close()

    @staticmethod
    def _orchestrate_end_to_end(deal):
        result = {
            "ssi_checked": False,
            "ssi_status": "UNKNOWN",
            "deal_status": deal.current_status,
            "buy_ssi_id": None,
            "sell_ssi_id": None,
            "fxtr014_id": None,
            "message_hub_status": None,
            "confirmed": False,
        }

        # Step 1: Ingest/Detect Deal Event
        try:
            url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/hub/deals"
            payload = DealService._build_message_hub_request(deal)
            mh_response = DealService._orchestrated_request(
                "POST", url, deal.deal_id, "POST /message-hub/api/v1/hub/deals", "message_hub", json_payload=payload
            )
            result["fxtr014_id"] = mh_response.get("id") or mh_response.get("fxtr014_id")
            result["message_hub_status"] = "FXTR014_GENERATED"
        except Exception as exc:
            logger.exception("Message Hub deal event ingestion failed for deal %s", deal.deal_id)
            result["message_hub_status"] = "FXTR014_GENERATION_FAILED"
            result["message_hub_error"] = str(exc)
            return result

        # Step 2: Resolve SSI (Settlement Engine check)
        try:
            ssi_url = f"{DealService._ssix_base_url().rstrip('/')}/api/v1/resolve-ssi"
            ssi_payload = DealService._build_ssi_request(deal)
            ssi_response = DealService._orchestrated_request(
                "POST", ssi_url, deal.deal_id, "POST /ssi/api/v1/resolve-ssi", "ssi_repo", json_payload=ssi_payload
            )
            result["ssi_checked"] = True
            result["ssi_status"] = ssi_response.get("status")
            result["buy_ssi_id"] = ssi_response.get("buy_ssi_id")
            result["sell_ssi_id"] = ssi_response.get("sell_ssi_id")
            
            # NEGATIVE ACK -> SSI UNMATCHED
            if result["ssi_status"] != "MATCHED":
                StatusTransitionService.transition_deal(deal, "FAILED", notes="SSI Resolution failed - UNMATCHED")
                TradeRepository.save(deal)
                
                DealService._update_pipeline_status(
                    deal_id=deal.deal_id,
                    status="SSI_UNMATCHED",
                    payload="SSI resolution returned UNMATCHED (Negative ACK)"
                )
                result["deal_status"] = "FAILED"
                return result
                
        except Exception as exc:
            logger.exception("SSI resolution failed for deal %s", deal.deal_id)
            StatusTransitionService.transition_deal(deal, "FAILED", notes=f"SSI Resolution exception: {str(exc)}")
            TradeRepository.save(deal)
            
            DealService._update_pipeline_status(
                deal_id=deal.deal_id,
                status="SSI_UNMATCHED",
                payload=f"SSI resolution exception: {str(exc)}"
            )
            result["deal_status"] = "FAILED"
            result["ssi_checked"] = True
            result["ssi_status"] = "ERROR"
            result["ssi_error"] = str(exc)
            return result

        # Step 3: Enrich FXTR014 (Positive ACK)
        try:
            enrich_url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/{result['fxtr014_id']}/enrich"
            enrich_params = {
                "cagm_ssi_id": result["buy_ssi_id"],
                "cagm_ssi_version": 1,
                "counterparty_ssi_id": result["sell_ssi_id"],
                "counterparty_ssi_version": 1,
            }
            enrich_response = DealService._orchestrated_request(
                "POST", enrich_url, deal.deal_id, f"POST /message-hub/api/v1/message-hub/fxtr014/{result['fxtr014_id']}/enrich", "message_hub", params=enrich_params
            )
            result["message_hub_status"] = "FXTR014_ENRICHED"
            result["enriched_fxtr014"] = enrich_response
        except Exception as exc:
            logger.exception("Message Hub enrichment failed for FXTR014 %s", result["fxtr014_id"])
            result["message_hub_status"] = "ENRICHMENT_FAILED"
            result["message_hub_error"] = str(exc)
            return result

        # Step 4: Generate MT300
        try:
            mt300_url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/{result['fxtr014_id']}/generate-mt300"
            mt300_response = DealService._orchestrated_request(
                "POST", mt300_url, deal.deal_id, f"POST /message-hub/api/v1/message-hub/fxtr014/{result['fxtr014_id']}/generate-mt300", "message_hub"
            )
            result["message_hub_status"] = "MT300_GENERATED"
            result["mt300_message"] = mt300_response.get("mt300_message")
        except Exception as exc:
            logger.exception("MT300 generation failed for FXTR014 %s", result["fxtr014_id"])
            result["message_hub_status"] = "MT300_GENERATION_FAILED"
            result["message_hub_error"] = str(exc)
            return result

        # Step 5: MT300 ACK (Dispatch and Confirmation)
        try:
            ack_url = f"{DealService._message_hub_base_url().rstrip('/')}/api/v1/message-hub/fxtr014/{result['fxtr014_id']}/mt300-ack"
            ack_response = DealService._orchestrated_request(
                "POST", ack_url, deal.deal_id, f"POST /message-hub/api/v1/message-hub/fxtr014/{result['fxtr014_id']}/mt300-ack", "message_hub"
            )
            result["message_hub_status"] = "MT300_DISPATCHED"
            result["deal_status"] = "CONFIRMED"
            result["confirmed"] = True
            result["mt300_ack_response"] = ack_response
        except Exception as exc:
            logger.exception("MT300 ACK processing failed for FXTR014 %s", result["fxtr014_id"])
            result["message_hub_status"] = "MT300_DISPATCH_FAILED"
            result["message_hub_error"] = str(exc)

        return result


    @staticmethod
    def process(xml_body):
        deal_dict = XMLParserService.parse_xml(xml_body)
        deal = Deal(**deal_dict)
        
        # DM-01: Duplicate Deal ID rejection
        if TradeRepository.exists(deal.deal_id):
            return {
                "status": "rejected",
                "reason": "Duplicate Deal ID - record already exists",
                "deal_id": deal.deal_id
            }
        
        # DM-02 & DM-03: Validation (silent filter + SPT enforcement)
        try:
            is_valid = ValidationService.validate(deal)
            if not is_valid:
                return {
                    "status": "rejected",
                    "reason": "Profit centre must be CAGM",
                    "deal_id": deal.deal_id
                }
        except ValueError as e:
            return {
                "status": "rejected",
                "reason": str(e),
                "deal_id": deal.deal_id
            }

        deal.current_status = "PENDING"
        now = datetime.utcnow()
        deal.last_status_at = now  # SL-02: Track when status set
        deal.ingested_at = now
        deal.is_immutable = True  # DM-04: Mark as immutable upon successful ingestion
        deal.set_retention_date()  # DM-06: Calculate 7-year retention date
        TradeRepository.save(deal)
        
        # Persist the inbound XML payload for the booked deal
        save_deal_xml(deal.deal_id, xml_body)

        # DM-05: Log status transition in audit trail
        AuditRepository.log_status_transition(deal.deal_id, None, "PENDING")

        payload = {k: v for k, v in deal.__dict__.items() if not k.startswith("_")}
        event = EventQueue(
            deal_id=deal.deal_id,
            event_type="New_Deal",
            payload=str(payload),
        )
        MessageRepository.publish(event)

        # Log the initial book deal API call
        try:
            import json
            DealService._log_api_trace(
                deal_id=deal.deal_id,
                endpoint="POST /trade/bookDeal",
                source="trade_repo",
                request_data=f"XML Payload:\n{xml_body}",
                response_data=f"Status: 200 OK\nBody:\n{json.dumps({'status': 'success', 'deal_id': deal.deal_id, 'deal_status': 'PENDING'}, indent=2)}"
            )
        except Exception:
            logger.exception("Failed to log bookDeal API trace")

        ssi_result = DealService._orchestrate_end_to_end(deal)

        return {
            "status": "success",
            "deal_id": deal.deal_id,
            **ssi_result,
        }

