from sqlalchemy.orm import DeclarativeBase, synonym
from sqlalchemy import Column, Integer, String, Float, Date, DateTime


class Base(DeclarativeBase):
    pass


class Deal(Base):
    __tablename__ = "deal_master"

    deal_id = Column(String(35), primary_key=True)
    trade_date = Column(Date, nullable=False)
    value_date = Column(Date, nullable=False)
    tran_type = Column(String(50), nullable=True)
    
    deal_status = Column("deal_status", String(50), nullable=False, default="PENDING")
    status = synonym("deal_status")
    
    counterparty = Column("counterparty", String(100), nullable=True)
    counterparty_name = synonym("counterparty")
    
    status_last_updated = Column("status_last_updated", DateTime, nullable=True)
    updated_at = synonym("status_last_updated")

    @property
    def counterparty_account(self):
        return None

    @counterparty_account.setter
    def counterparty_account(self, value):
        pass

    @property
    def ssi_type(self):
        return None

    @ssi_type.setter
    def ssi_type(self, value):
        pass
