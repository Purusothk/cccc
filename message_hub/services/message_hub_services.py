from sqlalchemy.orm import Session
from ..repos.message_repository import MessageRepository
from ..repos.pipeline_repository import PipelineRepository
from ..models.fxtr014_model import FXTR014
from ..models.pipeline_model import MessageStatusEnum
from ..schemas.fxtr014_schema import FXTR014Create, FXTR014Response
from datetime import datetime, date
from typing import Optional, Tuple
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import requests


class MessageHubService:
    """
    Message Hub Service for FX Trade Processing
    
    Responsibilities:
    1. Generate FXTR014 from deal economics
    2. Enrich FXTR014 with SSI details
    3. Generate MT300 from enriched FXTR014
    
    Flow:
    - Receives deal event from Trade Repository
    - Generates pre-enrichment FXTR014 (economics only)
    - Calls SSI Repository to resolve settlement legs
    - Enriches FXTR014 with SSI details
    - Generates MT300 for transmission via SWIFT
    """
    
    def __init__(self, db: Session):
        self.message_repo = MessageRepository(db)
        self.db = db
        self.pipeline_repo = PipelineRepository(db)
    
    def process_incoming_deal_event(self, deal_data: FXTR014Create, event_payload: str) -> FXTR014:
        """
        Process an incoming deal event from the trade repository.

        Creates a pipeline record, archives the received payload, then generates
        the pre-enrichment FXTR014 and updates the pipeline state.
        """
        pipeline_record = self.pipeline_repo.create_event(
            deal_id=deal_data.deal_id,
            message_type="DEAL_EVENT",
            payload=event_payload
        )

        try:
            fxtr014 = self.generate_fxtr014_from_deal(deal_data)
            self.pipeline_repo.update_status(
                deal_id=deal_data.deal_id,
                status=MessageStatusEnum.FXTR014_GENERATED.value,
                status_payload=f"FXTR014 generated for deal {deal_data.deal_id}"
            )
            return fxtr014
        except Exception as exc:
            self.pipeline_repo.update_status(
                deal_id=deal_data.deal_id,
                status=MessageStatusEnum.GENERATION_FAILED.value,
                last_error=str(exc)
            )
            raise

    def generate_fxtr014_from_deal(self, deal_data: FXTR014Create) -> FXTR014:
        """
        Step 3: Generate FXTR014 from deal economics
        
        Called when Message Hub receives deal event from Trade Repository.
        Creates pre-enrichment FXTR014 with economics data (rates, amounts, dates).
        
        Args:
            deal_data: Deal information from Trade Repository
            
        Returns:
            FXTR014 object in GENERATION_PENDING state
        """
        try:
            fxtr014 = self.message_repo.create_fxtr014_pre_enrichment(deal_data)
            
            # Generate pre-enrichment XML
            xml_content = self._generate_fxtr014_xml(
                fxtr014,
                include_ssi=False,
                include_mt300=False
            )
            
            self.message_repo.store_pre_enrichment_xml(fxtr014.id, xml_content)
            self.pipeline_repo.archive_message(
                deal_id=fxtr014.deal_id,
                message_type="FXTR014",
                payload=xml_content,
                archive_type="FXTR014_PRE_ENRICHMENT",
                message_status=MessageStatusEnum.FXTR014_GENERATED.value,
                counterparty=fxtr014.counterparty_bic_code,
                generated_at=datetime.utcnow()
            )
            
            return fxtr014
            
        except Exception as e:
            raise Exception(f"Failed to generate FXTR014: {str(e)}")
    
    def enrich_fxtr014_with_ssi(
        self,
        fxtr014_id: int,
        cagm_ssi_id: str,
        cagm_ssi_version: int,
        counterparty_ssi_id: str,
        counterparty_ssi_version: int
    ) -> FXTR014:
        """
        Step 4: Enrich FXTR014 with SSI Details
        
        Called after SSI Repository resolves both settlement legs.
        Adds SSI information (beneficiary details, account numbers, etc.)
        to the FXTR014.
        
        Args:
            fxtr014_id: ID of FXTR014 to enrich
            cagm_ssi_id: CAGM's SSI ID
            cagm_ssi_version: CAGM's SSI version
            counterparty_ssi_id: Counterparty's SSI ID
            counterparty_ssi_version: Counterparty's SSI version
            
        Returns:
            Enriched FXTR014 object in ENRICHED state
        """
        try:
            fxtr014 = self.message_repo.get_fxtr014_by_id(fxtr014_id)
            
            if not fxtr014:
                raise ValueError(f"FXTR014 with id {fxtr014_id} not found")
            
            # Update with SSI details
            enriched_fxtr014 = self.message_repo.update_fxtr014_enrichment(
                fxtr014_id,
                cagm_ssi_id,
                cagm_ssi_version,
                counterparty_ssi_id,
                counterparty_ssi_version
            )
            
            # Generate post-enrichment XML
            xml_content = self._generate_fxtr014_xml(
                enriched_fxtr014,
                include_ssi=True,
                include_mt300=False
            )
            
            enriched_fxtr014 = self.message_repo.store_post_enrichment_xml(
                fxtr014_id,
                xml_content
            )
            
            self.pipeline_repo.archive_message(
                deal_id=enriched_fxtr014.deal_id,
                message_type="FXTR014",
                payload=xml_content,
                archive_type="FXTR014_POST_ENRICHMENT",
                message_status=MessageStatusEnum.SSI_ENRICHED.value,
                counterparty=enriched_fxtr014.counterparty_ssi_id,
                generated_at=datetime.utcnow()
            )
            
            self.pipeline_repo.update_status(
                deal_id=fxtr014.deal_id,
                status=MessageStatusEnum.SSI_ENRICHED.value,
                ssi_type="MATCHED",
                ssi_version_buy_id=cagm_ssi_id,
                ssi_version_sell_id=counterparty_ssi_id,
                status_payload=f"SSI matched: {cagm_ssi_id} / {counterparty_ssi_id}"
            )
            
            return enriched_fxtr014
            
        except Exception as e:
            raise Exception(f"Failed to enrich FXTR014: {str(e)}")
    
    def generate_mt300_from_fxtr014(self, fxtr014_id: int) -> str:
        """
        Step 5: Generate MT300 from Enriched FXTR014
        
        Called after enrichment to generate SWIFT MT300 message.
        MT300 is routed via CAB SWIFT infrastructure to counterparty.
        
        Args:
            fxtr014_id: ID of enriched FXTR014
            
        Returns:
            MT300 message as string
        """
        try:
            fxtr014 = self.message_repo.get_fxtr014_by_id(fxtr014_id)
            
            if not fxtr014:
                raise ValueError(f"FXTR014 with id {fxtr014_id} not found")
            
            if fxtr014.state != "ENRICHED":
                raise ValueError(f"FXTR014 must be in ENRICHED state. Current state: {fxtr014.state}")
            
            # Generate MT300
            mt300_content = self._generate_mt300_swift_message(fxtr014)
            self.pipeline_repo.archive_message(
                deal_id=fxtr014.deal_id,
                message_type="MT300",
                payload=mt300_content,
                archive_type="MT300_GENERATED",
                message_status=MessageStatusEnum.MT300_GENERATED.value,
                counterparty=fxtr014.counterparty_bic_code or fxtr014.counterparty_ssi_id,
                generated_at=datetime.utcnow()
            )
            self.pipeline_repo.update_status(
                deal_id=fxtr014.deal_id,
                status=MessageStatusEnum.MT300_GENERATED.value,
                status_payload=f"MT300 generated for FXTR014 {fxtr014.id}"
            )
            return mt300_content
            
        except Exception as e:
            raise Exception(f"Failed to generate MT300: {str(e)}")

    def dispatch_mt300(self, fxtr014_id: int) -> str:
        """
        Dispatch an already-generated MT300 for the FXTR014.

        This marks the MT300 as dispatched and archives the raw message.
        """
        try:
            fxtr014 = self.message_repo.get_fxtr014_by_id(fxtr014_id)
            if not fxtr014:
                raise ValueError(f"FXTR014 with id {fxtr014_id} not found")
            if fxtr014.state != "ENRICHED":
                raise ValueError(f"FXTR014 must be in ENRICHED state to dispatch MT300. Current state: {fxtr014.state}")

            mt300_content = self._generate_mt300_swift_message(fxtr014)
            self.pipeline_repo.archive_message(
                deal_id=fxtr014.deal_id,
                message_type="MT300",
                payload=mt300_content,
                archive_type="MT300_DISPATCHED",
                message_status=MessageStatusEnum.MT300_DISPATCHED.value,
                counterparty=fxtr014.counterparty_bic_code or fxtr014.counterparty_ssi_id,
                generated_at=fxtr014.generated_at,
                dispatched_at=datetime.utcnow()
            )
            record = self.pipeline_repo.update_status(
                deal_id=fxtr014.deal_id,
                status=MessageStatusEnum.MT300_DISPATCHED.value
            )
            if not record:
                raise ValueError(f"Pipeline record not found for deal {fxtr014.deal_id}")
            return mt300_content
        except Exception as e:
            raise Exception(f"Failed to dispatch MT300: {str(e)}")
    
    def generate_pain001_from_fxtr014(self, fxtr014_id: int) -> str:
        """
        Generate PAIN.001 XML from an enriched FXTR014.

        Validates the generated XML and archives the raw PAIN.001 payload.
        """
        try:
            fxtr014 = self.message_repo.get_fxtr014_by_id(fxtr014_id)
            if not fxtr014:
                raise ValueError(f"FXTR014 with id {fxtr014_id} not found")
            if fxtr014.state != "ENRICHED":
                raise ValueError(f"FXTR014 must be in ENRICHED state. Current state: {fxtr014.state}")

            pain001_xml = self._generate_pain001_xml(fxtr014)
            self._validate_pain001_xml(pain001_xml)

            self.pipeline_repo.archive_message(
                deal_id=fxtr014.deal_id,
                message_type="PAIN001",
                payload=pain001_xml,
                archive_type="PAIN001_GENERATED",
                message_status=MessageStatusEnum.PAIN001_GENERATED.value,
                counterparty=fxtr014.counterparty_ssi_bic or fxtr014.counterparty_ssi_id,
                generated_at=datetime.utcnow()
            )
            self.pipeline_repo.update_status(
                deal_id=fxtr014.deal_id,
                status=MessageStatusEnum.PAIN001_GENERATED.value,
                status_payload=f"PAIN.001 generated for FXTR014 {fxtr014.id}"
            )
            return pain001_xml
        except Exception as e:
            raise Exception(f"Failed to generate PAIN.001: {str(e)}")

    def submit_pain001(self, fxtr014_id: int) -> str:
        """
        Submit PAIN.001 for an enriched FXTR014.

        This validates the PAIN.001 XML, archives the submitted payload, and
        transitions pipeline state to PAIN001_SUBMITTED.
        """
        try:
            fxtr014 = self.message_repo.get_fxtr014_by_id(fxtr014_id)
            if not fxtr014:
                raise ValueError(f"FXTR014 with id {fxtr014_id} not found")
            if fxtr014.state != "ENRICHED":
                raise ValueError(f"FXTR014 must be in ENRICHED state. Current state: {fxtr014.state}")

            pain001_xml = self._generate_pain001_xml(fxtr014)
            self._validate_pain001_xml(pain001_xml)

            self.pipeline_repo.archive_message(
                deal_id=fxtr014.deal_id,
                message_type="PAIN001",
                payload=pain001_xml,
                archive_type="PAIN001_SUBMITTED",
                message_status=MessageStatusEnum.PAIN001_SUBMITTED.value,
                counterparty=fxtr014.counterparty_ssi_bic or fxtr014.counterparty_ssi_id,
                generated_at=datetime.utcnow(),
                dispatched_at=datetime.utcnow()
            )
            self.pipeline_repo.update_status(
                deal_id=fxtr014.deal_id,
                status=MessageStatusEnum.PAIN001_SUBMITTED.value,
                status_payload=f"PAIN.001 submitted for FXTR014 {fxtr014.id}"
            )
            return pain001_xml
        except Exception as e:
            self.pipeline_repo.update_status(
                deal_id=fxtr014.deal_id if 'fxtr014' in locals() and fxtr014 else None,
                status=MessageStatusEnum.PAIN001_SUBMIT_FAILED.value,
                last_error=str(e)
            )
            raise Exception(f"Failed to submit PAIN.001: {str(e)}")

    @staticmethod
    def _trade_repo_base_url():
        return os.getenv("TRADE_REPO_API_BASE_URL", "http://127.0.0.1:8000/trade")

    @staticmethod
    def _trade_repo_headers():
        return {"Content-Type": "application/json"}

    def update_trade_repo_status(self, deal_id: str, new_status: str):
        url = f"{self._trade_repo_base_url().rstrip('/')}/api/v1/deals/{deal_id}/status"
        payload = {"new_status": new_status}
        response = requests.post(url, json=payload, headers=self._trade_repo_headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def run_pain001_scheduler(self, as_of: Optional[date] = None, submit: bool = True) -> dict:
        as_of = as_of or date.today()
        pipeline_records = self.pipeline_repo.get_by_status(MessageStatusEnum.MT300_DISPATCHED.value)
        result = {
            "evaluated_count": len(pipeline_records),
            "generated_count": 0,
            "submitted_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "details": []
        }

        for record in pipeline_records:
            fxtr014 = self.message_repo.get_fxtr014_by_deal_id(record.deal_id)
            if not fxtr014:
                result["failed_count"] += 1
                result["details"].append(f"Missing FXTR014 for deal {record.deal_id}")
                continue

            fxtr_value_date = fxtr014.value_date
            if fxtr_value_date and hasattr(fxtr_value_date, "date"):
                fxtr_value_date = fxtr_value_date.date()
            as_of_date = as_of
            if as_of_date and hasattr(as_of_date, "date"):
                as_of_date = as_of_date.date()

            if not fxtr_value_date or fxtr_value_date > as_of_date:
                result["skipped_count"] += 1
                continue

            if record.message_status != MessageStatusEnum.MT300_DISPATCHED.value:
                result["skipped_count"] += 1
                continue

            try:
                pain_xml = self.generate_pain001_from_fxtr014(fxtr014.id)
                result["generated_count"] += 1
                
                # Log PAIN.001 Generation to StatusMonitor
                try:
                    from ..models.pipeline_model import StatusMonitor
                    monitor_gen = StatusMonitor(
                        deal_id=fxtr014.deal_id,
                        status="POST /fxtr014/{id}/generate-pain001",
                        source="message_hub",
                        payload=f"Request Details:\nMethod: POST\nURL: http://127.0.0.1:8000/message-hub/api/v1/message-hub/fxtr014/{fxtr014.id}/generate-pain001\n\nResponse Details:\nStatus: 200 OK\nPayload:\n{pain_xml}",
                        created_at=datetime.utcnow()
                    )
                    self.db.add(monitor_gen)
                    self.db.commit()
                except Exception:
                    logger.exception("Failed to write PAIN.001 gen status monitor log")

                if submit:
                    self.submit_pain001(fxtr014.id)
                    result["submitted_count"] += 1
                    
                    # Log PAIN.001 Submission to StatusMonitor
                    try:
                        from ..models.pipeline_model import StatusMonitor
                        monitor_sub = StatusMonitor(
                            deal_id=fxtr014.deal_id,
                            status="POST /fxtr014/{id}/submit-pain001",
                            source="message_hub",
                            payload=f"Request Details:\nMethod: POST\nURL: http://127.0.0.1:8000/message-hub/api/v1/message-hub/fxtr014/{fxtr014.id}/submit-pain001\n\nResponse Details:\nStatus: 200 OK\nPayload:\n{pain_xml}",
                            created_at=datetime.utcnow()
                        )
                        self.db.add(monitor_sub)
                        self.db.commit()
                    except Exception:
                        logger.exception("Failed to write PAIN.001 submit status monitor log")

                    try:
                        ack_res = self.update_trade_repo_status(fxtr014.deal_id, "PAYMENT_INITIATED")
                        
                        # Log Trade Repo status update to StatusMonitor
                        try:
                            from ..models.pipeline_model import StatusMonitor
                            import json
                            monitor_tr = StatusMonitor(
                                deal_id=fxtr014.deal_id,
                                status="POST /trade/api/v1/deals/{id}/status",
                                source="trade_repo",
                                payload=f"Request Details:\nMethod: POST\nURL: http://127.0.0.1:8000/trade/api/v1/deals/{fxtr014.deal_id}/status\nBody:\n{{\n  \"new_status\": \"PAYMENT_INITIATED\"\n}}\n\nResponse Details:\nStatus: 200 OK\nBody:\n{json.dumps(ack_res, indent=2)}",
                                created_at=datetime.utcnow()
                            )
                            self.db.add(monitor_tr)
                            self.db.commit()
                        except Exception:
                            logger.exception("Failed to write PAYMENT_INITIATED status monitor log")
                    except Exception as ack_exc:
                        result["failed_count"] += 1
                        result["details"].append(
                            f"Deal {record.deal_id}: payment initiation status update failed - {str(ack_exc)}"
                        )
            except Exception as exc:
                result["failed_count"] += 1
                result["details"].append(f"Deal {record.deal_id}: {str(exc)}")

        self.pipeline_repo.save_scheduler_log(
            evaluated_count=result["evaluated_count"],
            generated_count=result["generated_count"],
            submitted_count=result["submitted_count"],
            skipped_count=result["skipped_count"],
            failed_count=result["failed_count"],
            details="; ".join(result["details"]) or None
        )
        return result

    def _generate_pain001_xml(self, fxtr014: FXTR014) -> str:
        """
        Build a simplified PAIN.001 XML payload from an enriched FXTR014.
        """
        root = ET.Element('Document', xmlns='urn:iso:std:iso:20022:tech:xsd:pain.001.001.03')
        cstmr_cdt_trf_initn = ET.SubElement(root, 'CstmrCdtTrfInitn')

        grp_hdr = ET.SubElement(cstmr_cdt_trf_initn, 'GrpHdr')
        ET.SubElement(grp_hdr, 'MsgId').text = f"PAIN001-{fxtr014.deal_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        ET.SubElement(grp_hdr, 'CreDtTm').text = datetime.utcnow().isoformat()
        ET.SubElement(grp_hdr, 'NbOfTxs').text = '1'
        ET.SubElement(grp_hdr, 'CtrlSum').text = f"{fxtr014.ctr_sell_amount:.2f}"

        initg_pty = ET.SubElement(grp_hdr, 'InitgPty')
        ET.SubElement(initg_pty, 'Nm').text = fxtr014.cagm_bic_code or 'CAGM'

        pmt_inf = ET.SubElement(cstmr_cdt_trf_initn, 'PmtInf')
        ET.SubElement(pmt_inf, 'PmtInfId').text = f"PAYMENT-{fxtr014.deal_id}"
        ET.SubElement(pmt_inf, 'PmtMtd').text = 'TRF'
        ET.SubElement(pmt_inf, 'BtchBookg').text = 'false'
        ET.SubElement(pmt_inf, 'NbOfTxs').text = '1'
        ET.SubElement(pmt_inf, 'CtrlSum').text = f"{fxtr014.ctr_sell_amount:.2f}"

        pmt_tp_inf = ET.SubElement(pmt_inf, 'PmtTpInf')
        svc_lvl = ET.SubElement(pmt_tp_inf, 'SvcLvl')
        ET.SubElement(svc_lvl, 'Cd').text = 'SEPA'

        reqd_exctn_dt = ET.SubElement(pmt_inf, 'ReqdExctnDt')
        reqd_exctn_dt.text = fxtr014.value_date.date().isoformat()

        dbtr = ET.SubElement(pmt_inf, 'Dbtr')
        ET.SubElement(dbtr, 'Nm').text = fxtr014.cagm_bic_code or 'CAGM'

        dbtr_acct = ET.SubElement(pmt_inf, 'DbtrAcct')
        id_el = ET.SubElement(dbtr_acct, 'Id')
        ET.SubElement(id_el, 'IBAN').text = fxtr014.cagm_ssi_id or 'UNKNOWN'

        cdtr = ET.SubElement(pmt_inf, 'Cdtr')
        ET.SubElement(cdtr, 'Nm').text = fxtr014.counterparty_ssi_name or fxtr014.counterparty_ssi_id or 'COUNTERPARTY'

        cdtr_acct = ET.SubElement(pmt_inf, 'CdtrAcct')
        id_el2 = ET.SubElement(cdtr_acct, 'Id')
        ET.SubElement(id_el2, 'IBAN').text = fxtr014.counterparty_ssi_iban or 'UNKNOWN'

        amt = ET.SubElement(pmt_inf, 'Amt')
        instd_amt = ET.SubElement(amt, 'InstdAmt', Ccy=fxtr014.ctr_sell_currency)
        instd_amt.text = f"{fxtr014.ctr_sell_amount:.2f}"

        rmt_inf = ET.SubElement(pmt_inf, 'RmtInf')
        ET.SubElement(rmt_inf, 'Ustrd').text = f"Settlement for deal {fxtr014.deal_id}"

        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent='  ')
        return xml_str

    def _validate_pain001_xml(self, xml_content: str):
        """
        Validate the PAIN.001 XML by parsing and checking for required elements.
        """
        try:
            tree = ET.fromstring(xml_content)

            # handle namespaced root tags like {urn:...}Document
            root_tag = tree.tag
            root_local = root_tag.split('}', 1)[1] if '}' in root_tag else root_tag
            if root_local != 'Document':
                raise ValueError('Invalid PAIN.001 root element')

            # helper to find element by local-name anywhere in tree
            def has_local(elem, localname):
                for node in elem.iter():
                    t = node.tag
                    name = t.split('}', 1)[1] if '}' in t else t
                    if name == localname:
                        return True
                return False

            if not has_local(tree, 'GrpHdr') or not has_local(tree, 'PmtInf'):
                raise ValueError('PAIN.001 XML is missing required group header or payment information')
        except ET.ParseError as e:
            raise ValueError(f'Invalid PAIN.001 XML: {str(e)}')

    def _generate_fxtr014_xml(
        self, 
        fxtr014: FXTR014,
        include_ssi: bool = False,
        include_mt300: bool = False
    ) -> str:
        """
        Generate FXTR014 XML payload
        
        Mapping to FXTR014 Elements:
        - TradInf/TradDt: trade_date (ISO 8601)
        - TradInf/OrgrRef: deal_id (Deal ID as originator reference)
        - TradInf/OprTp: operation_type (NEWT for new trade)
        - TradgSdId/SubmitgPty: CAGM BIC (CAGM BIC from config)
        - CtPtySdId/SubmitgPty: Counterparty BIC (from SSI if enriched)
        - TradAmts/TradgSdBuyAmt: cagm_buy_amount
        - TradAmts/TradgSdSellAmt: ctr_sell_amount
        - TradAmts/StlmDt: value_date (Settlement date)
        - AgrdrRate/XchgRate: spot_rate
        - TradgSdStlmInstrs: CAGM SSI (if enriched)
        - CtPtySdStlmInstrs: Counterparty SSI (if enriched)
        """
        root = ET.Element('FXTR014', {
            'version': '1.0',
            'state': fxtr014.state,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Trade Information
        trad_inf = ET.SubElement(root, 'TradInf')
        ET.SubElement(trad_inf, 'TradDt').text = fxtr014.trade_date.isoformat()
        ET.SubElement(trad_inf, 'OrgrRef').text = fxtr014.deal_id
        ET.SubElement(trad_inf, 'OprTp').text = fxtr014.operation_type
        
        # CAGM Leg (Selling leg)
        tradg_sd = ET.SubElement(root, 'TradgSdId')
        ET.SubElement(tradg_sd, 'SubmitgPty').text = fxtr014.cagm_bic_code or "UNKNOWN"
        
        # Counterparty Leg (Buying leg)
        ctr_pty_sd = ET.SubElement(root, 'CtPtySdId')
        ET.SubElement(ctr_pty_sd, 'SubmitgPty').text = fxtr014.counterparty_bic_code or "UNKNOWN"
        
        # Trade Amounts
        trad_amts = ET.SubElement(root, 'TradAmts')
        ET.SubElement(trad_amts, 'TradgSdBuyAmt').text = str(fxtr014.cagm_buy_amount)
        ET.SubElement(trad_amts, 'TradgSdBuyCcy').text = fxtr014.cagm_buy_currency
        ET.SubElement(trad_amts, 'TradgSdSellAmt').text = str(fxtr014.ctr_sell_amount)
        ET.SubElement(trad_amts, 'TradgSdSellCcy').text = fxtr014.ctr_sell_currency
        ET.SubElement(trad_amts, 'StlmDt').text = fxtr014.value_date.isoformat()
        
        # Exchange Rate
        agrdr_rate = ET.SubElement(root, 'AgrdrRate')
        ET.SubElement(agrdr_rate, 'XchgRate').text = str(fxtr014.spot_rate)
        
        # Settlement Instructions (if enriched)
        if include_ssi and fxtr014.state == "ENRICHED":
            tradg_sd_stlm = ET.SubElement(root, 'TradgSdStlmInstrs')
            ET.SubElement(tradg_sd_stlm, 'SsiId').text = fxtr014.cagm_ssi_id or ""
            ET.SubElement(tradg_sd_stlm, 'SsiVersion').text = str(fxtr014.cagm_ssi_version)
            
            ctr_pty_stlm = ET.SubElement(root, 'CtPtySdStlmInstrs')
            ET.SubElement(ctr_pty_stlm, 'SsiId').text = fxtr014.counterparty_ssi_id or ""
            ET.SubElement(ctr_pty_stlm, 'SsiVersion').text = str(fxtr014.counterparty_ssi_version)
        
        # Pretty print XML
        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
        return xml_str
    
    def _generate_mt300_swift_message(self, fxtr014: FXTR014) -> str:
        """
        Generate MT300 SWIFT Message from Enriched FXTR014
        
        MT300 is the SWIFT message for FX Confirmations.
        Structure:
        - Headers
        - Trade Date, Value Date
        - Currency amounts and rate
        - Counterparty details
        - Settlement instructions
        """
        # MT300 basic template
        mt300 = f"""
{'{'}:20:{fxtr014.trade_id}{'}'}
{'{'}:21:{fxtr014.deal_id}{'}'}
{'{'}:30:{fxtr014.trade_date.strftime('%y%m%d')}{'}'}
{'{'}:31:{fxtr014.value_date.strftime('%y%m%d')}{'}'}
{'{'}:32:{fxtr014.cagm_buy_currency}{fxtr014.cagm_buy_amount:.2f}{'}'}
{'{'}:33:{fxtr014.ctr_sell_currency}{fxtr014.ctr_sell_amount:.2f}{'}'}
{'{'}:36:{fxtr014.spot_rate:.6f}{'}'}
{'{'}:56:{fxtr014.cagm_bic_code or 'UNKNOWN'}{'}'}
{'{'}:57:{fxtr014.counterparty_bic_code or 'UNKNOWN'}{'}'}
{'{'}:72:FX TRADE SETTLEMENT FOR DEAL {fxtr014.deal_id}{'}'}
        """.strip()
        
        # Add SSI if available
        if fxtr014.cagm_ssi_id:
            mt300 += f"\n{'{'}:50A:CAGM SSI {fxtr014.cagm_ssi_id}{'}'}"
        
        if fxtr014.counterparty_ssi_id:
            mt300 += f"\n{'{'}:50B:COUNTERPARTY SSI {fxtr014.counterparty_ssi_id}{'}'}"
        
        return mt300
    
    def get_fxtr014_details(self, fxtr014_id: int) -> FXTR014Response:
        """Get complete FXTR014 details"""
        fxtr014 = self.message_repo.get_fxtr014_by_id(fxtr014_id)
        
        if not fxtr014:
            raise ValueError(f"FXTR014 with id {fxtr014_id} not found")
        
        return FXTR014Response.from_orm(fxtr014)

