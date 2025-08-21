import pandas as pd
import pytz
from datetime import datetime, timedelta, time as dt_time
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from app.models import StoreStatus, BusinessHours, StoreTimezone, Report
from app.database import get_db
import csv
import os
from typing import Dict, List, Tuple
import asyncio

class ReportGeneratorService:
    def __init__(self):
        self.reports_dir = "reports"
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)
    
    def get_max_timestamp(self, db: Session) -> datetime:
        """Get the maximum timestamp from store status data"""
        max_timestamp = db.query(func.max(StoreStatus.timestamp_utc)).scalar()
        return max_timestamp
    
    def get_store_timezone(self, db: Session, store_id: str) -> str:
        """Get timezone for a store, default to America/Chicago"""
        timezone_data = db.query(StoreTimezone).filter(
            StoreTimezone.store_id == store_id
        ).first()
        
        return timezone_data.timezone_str if timezone_data else "America/Chicago"
    
    def get_business_hours(self, db: Session, store_id: str) -> Dict:
        """Get business hours for a store, default to 24/7"""
        business_hours = db.query(BusinessHours).filter(
            BusinessHours.store_id == store_id
        ).all()
        
        if not business_hours:
            # Default 24/7
            return {i: (dt_time(0, 0), dt_time(23, 59, 59)) for i in range(7)}
        
        hours_dict = {}
        for bh in business_hours:
            hours_dict[bh.day_of_week] = (bh.start_time_local, bh.end_time_local)
        
        return hours_dict
    
    def convert_utc_to_local(self, utc_time: datetime, timezone_str: str) -> datetime:
        """Convert UTC time to local timezone"""
        utc_time = pytz.UTC.localize(utc_time) if utc_time.tzinfo is None else utc_time
        local_tz = pytz.timezone(timezone_str)
        return utc_time.astimezone(local_tz)
    
    def is_within_business_hours(self, local_time: datetime, business_hours: Dict) -> bool:
        """Check if given time is within business hours"""
        day_of_week = local_time.weekday()  # 0=Monday, 6=Sunday
        
        if day_of_week not in business_hours:
            return False
        
        start_time, end_time = business_hours[day_of_week]
        current_time = local_time.time()
        
        # Handle overnight business hours
        if start_time <= end_time:
            return start_time <= current_time <= end_time
        else:  # Crosses midnight
            return current_time >= start_time or current_time <= end_time
    
    def get_business_hours_duration(self, business_hours: Dict, day_of_week: int) -> float:
        """Calculate business hours duration for a day in hours"""
        if day_of_week not in business_hours:
            return 0.0
        
        start_time, end_time = business_hours[day_of_week]
        
        # Convert to minutes for calculation
        start_minutes = start_time.hour * 60 + start_time.minute
        end_minutes = end_time.hour * 60 + end_time.minute
        
        if start_minutes <= end_minutes:
            duration_minutes = end_minutes - start_minutes
        else:  # Crosses midnight
            duration_minutes = (24 * 60) - start_minutes + end_minutes
        
        return duration_minutes / 60.0  # Convert to hours
    
    def interpolate_status_for_period(
        self, 
        db: Session, 
        store_id: str, 
        start_time: datetime, 
        end_time: datetime,
        timezone_str: str,
        business_hours: Dict
    ) -> Tuple[float, float]:
        """
        Interpolate uptime and downtime for a given period
        Returns (uptime_hours, downtime_hours)
        """
        # Get all status records for this store in the time period
        status_records = db.query(StoreStatus).filter(
            and_(
                StoreStatus.store_id == store_id,
                StoreStatus.timestamp_utc >= start_time,
                StoreStatus.timestamp_utc <= end_time
            )
        ).order_by(StoreStatus.timestamp_utc).all()
        
        if not status_records:
            # No data available, assume store was inactive
            return 0.0, 0.0
        
        total_uptime_minutes = 0.0
        total_downtime_minutes = 0.0
        
        # Process each day in the period
        current_date = start_time.date()
        end_date = end_time.date()
        
        while current_date <= end_date:
            day_start = datetime.combine(current_date, dt_time.min)
            day_end = datetime.combine(current_date, dt_time.max)
            
            # Convert to local time for business hours check
            local_day_start = self.convert_utc_to_local(day_start, timezone_str)
            day_of_week = local_day_start.weekday()
            
            if day_of_week not in business_hours:
                current_date += timedelta(days=1)
                continue
            
            # Get business hours for this day
            bh_start, bh_end = business_hours[day_of_week]
            
            # Create business hours datetime objects
            bh_start_dt = datetime.combine(local_day_start.date(), bh_start)
            bh_end_dt = datetime.combine(local_day_start.date(), bh_end)
            
            # Handle overnight business hours
            if bh_start > bh_end:
                bh_end_dt += timedelta(days=1)
            
            # Convert back to UTC
            local_tz = pytz.timezone(timezone_str)
            bh_start_utc = local_tz.localize(bh_start_dt).astimezone(pytz.UTC)
            bh_end_utc = local_tz.localize(bh_end_dt).astimezone(pytz.UTC)
            
            # Adjust for the actual period boundaries
            period_start = max(start_time, bh_start_utc.replace(tzinfo=None))
            period_end = min(end_time, bh_end_utc.replace(tzinfo=None))
            
            if period_start >= period_end:
                current_date += timedelta(days=1)
                continue
            
            # Get status records for this business day
            day_records = [r for r in status_records 
                          if period_start <= r.timestamp_utc <= period_end]
            
            if not day_records:
                # No observations, assume downtime
                duration = (period_end - period_start).total_seconds() / 60
                total_downtime_minutes += duration
            else:
                # Interpolate based on available observations
                last_timestamp = period_start
                last_status = 'inactive'  # Assume inactive at start
                
                for record in day_records:
                    # Duration since last timestamp
                    duration = (record.timestamp_utc - last_timestamp).total_seconds() / 60
                    
                    if last_status == 'active':
                        total_uptime_minutes += duration
                    else:
                        total_downtime_minutes += duration
                    
                    last_timestamp = record.timestamp_utc
                    last_status = record.status
                
                # Handle remaining time until period end
                remaining_duration = (period_end - last_timestamp).total_seconds() / 60
                if last_status == 'active':
                    total_uptime_minutes += remaining_duration
                else:
                    total_downtime_minutes += remaining_duration
            
            current_date += timedelta(days=1)
        
        # Convert minutes to hours
        return total_uptime_minutes / 60.0, total_downtime_minutes / 60.0
    
    def calculate_store_metrics(self, db: Session, store_id: str, current_time: datetime) -> Dict:
        """Calculate uptime/downtime metrics for a store"""
        timezone_str = self.get_store_timezone(db, store_id)
        business_hours = self.get_business_hours(db, store_id)
        
        # Define time periods
        one_hour_ago = current_time - timedelta(hours=1)
        one_day_ago = current_time - timedelta(days=1)
        one_week_ago = current_time - timedelta(weeks=1)
        
        # Calculate metrics for each period
        uptime_1h, downtime_1h = self.interpolate_status_for_period(
            db, store_id, one_hour_ago, current_time, timezone_str, business_hours
        )
        
        uptime_1d, downtime_1d = self.interpolate_status_for_period(
            db, store_id, one_day_ago, current_time, timezone_str, business_hours
        )
        
        uptime_1w, downtime_1w = self.interpolate_status_for_period(
            db, store_id, one_week_ago, current_time, timezone_str, business_hours
        )
        
        return {
            'store_id': store_id,
            'uptime_last_hour': round(uptime_1h * 60, 2),  # Convert to minutes
            'uptime_last_day': round(uptime_1d, 2),
            'uptime_last_week': round(uptime_1w, 2),
            'downtime_last_hour': round(downtime_1h * 60, 2),  # Convert to minutes
            'downtime_last_day': round(downtime_1d, 2),
            'downtime_last_week': round(downtime_1w, 2)
        }
    
    async def generate_report_async(self, report_id: str):
        """Generate report asynchronously"""
        db = next(get_db())
        
        try:
            # Update report status
            report = db.query(Report).filter(Report.id == report_id).first()
            if not report:
                return
            
            # Get current timestamp (max timestamp in data)
            current_time = self.get_max_timestamp(db)
            
            # Get all unique store IDs
            store_ids = db.query(StoreStatus.store_id).distinct().all()
            store_ids = [sid[0] for sid in store_ids]
            
            # Generate report data
            report_data = []
            for store_id in store_ids:
                try:
                    metrics = self.calculate_store_metrics(db, store_id, current_time)
                    report_data.append(metrics)
                except Exception as e:
                    print(f"Error processing store {store_id}: {e}")
                    continue
            
            # Save to CSV
            csv_file_path = os.path.join(self.reports_dir, f"{report_id}.csv")
            
            with open(csv_file_path, 'w', newline='') as csvfile:
                fieldnames = [
                    'store_id', 'uptime_last_hour', 'uptime_last_day', 
                    'uptime_last_week', 'downtime_last_hour', 
                    'downtime_last_day', 'downtime_last_week'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(report_data)
            
            # Update report status
            report.status = "Complete"
            report.file_path = csv_file_path
            db.commit()
            
        except Exception as e:
            print(f"Error generating report: {e}")
            report.status = "Failed"
            db.commit()
        finally:
            db.close()
