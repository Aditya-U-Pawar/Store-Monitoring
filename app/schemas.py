from pydantic import BaseModel
from datetime import datetime, time
from typing import Optional

class StoreStatusResponse(BaseModel):
    store_id: str
    uptime_last_hour: float
    uptime_last_day: float
    uptime_last_week: float
    downtime_last_hour: float
    downtime_last_day: float
    downtime_last_week: float

class ReportTriggerResponse(BaseModel):
    report_id: str

class ReportStatusResponse(BaseModel):
    status: str
    csv_data: Optional[str] = None
