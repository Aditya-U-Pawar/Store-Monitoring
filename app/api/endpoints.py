from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Report
from app.schemas import ReportTriggerResponse, ReportStatusResponse
from app.services.report_generator import ReportGeneratorService
import uuid
import os

router = APIRouter()
report_service = ReportGeneratorService()

@router.post("/trigger_report", response_model=ReportTriggerResponse)
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Trigger report generation"""
    # Create new report record
    report_id = str(uuid.uuid4())
    report = Report(id=report_id, status="Running")
    
    db.add(report)
    db.commit()
    
    # Start background task for report generation
    background_tasks.add_task(report_service.generate_report_async, report_id)
    
    return ReportTriggerResponse(report_id=report_id)

@router.get("/get_report")
async def get_report(report_id: str, db: Session = Depends(get_db)):
    """Get report status or download CSV"""
    report = db.query(Report).filter(Report.id == report_id).first()
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    if report.status == "Running":
        return {"status": "Running"}
    elif report.status == "Complete":
        if report.file_path and os.path.exists(report.file_path):
            return FileResponse(
                path=report.file_path,
                media_type='text/csv',
                filename=f"store_report_{report_id}.csv"
            )
        else:
            raise HTTPException(status_code=500, detail="Report file not found")
    else:
        raise HTTPException(status_code=500, detail="Report generation failed")
