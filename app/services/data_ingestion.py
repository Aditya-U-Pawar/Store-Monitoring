import pandas as pd
import pytz
from datetime import datetime, time
from sqlalchemy.orm import Session
from app.models import StoreStatus, BusinessHours, StoreTimezone
from app.database import get_db, engine, Base
import requests
import zipfile
import os

class DataIngestionService:
    def __init__(self):
        self.data_url = "https://storage.googleapis.com/hiring-problem-statements/store-monitoring-data.zip"
    
    def download_and_extract_data(self):
        """Download and extract CSV files"""
        print("📥 Starting data download...")
        
        if not os.path.exists("data"):
            os.makedirs("data")
        
        zip_path = "data/store_data.zip"
        
        # Check if zip already exists
        if os.path.exists(zip_path):
            print("📁 Using existing zip file...")
        else:
            print("🌐 Downloading data from Google Cloud Storage...")
            try:
                response = requests.get(self.data_url, timeout=30)
                response.raise_for_status()
                
                with open(zip_path, "wb") as f:
                    f.write(response.content)
                print(f"✅ Downloaded {len(response.content)} bytes")
            except Exception as e:
                print(f"❌ Download failed: {e}")
                return False
        
        # Extract zip file
        print("📦 Extracting CSV files...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall("data/")
            
            # List extracted files
            files = [f for f in os.listdir("data") if f.endswith('.csv')]
            print(f"📊 Extracted CSV files: {files}")
            
            return True
        except Exception as e:
            print(f"❌ Extraction failed: {e}")
            return False
    
    def find_csv_file(self, possible_names):
        """Find CSV file with any of the possible names"""
        for name in possible_names:
            path = os.path.join("data", name)
            if os.path.exists(path):
                return path
        return None
    
    def ingest_store_status(self, db: Session):
        """Ingest store status data from CSV using bulk operations"""
        print("🏪 Processing store status data...")
        
        # Try different possible file names
        possible_names = [
            "store_status.csv",
            "store status.csv", 
            "Store Status.csv",
            "store_data.csv"
        ]
        
        file_path = self.find_csv_file(possible_names)
        if not file_path:
            print(f"❌ Store status file not found. Tried: {possible_names}")
            return 0
        
        print(f"📖 Reading from: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            print(f"📊 Loaded {len(df)} store status records")
            print(f"📋 Columns: {list(df.columns)}")
            
            # Handle different column name variations
            if 'timestamp_utc' not in df.columns:
                if 'timestamp' in df.columns:
                    df['timestamp_utc'] = df['timestamp']
                else:
                    print("❌ No timestamp column found")
                    return 0
            
            # Clean and prepare data
            df['store_id'] = df['store_id'].astype(str)
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
            df['status'] = df['status'].astype(str)
            
            # Remove any rows with null values
            initial_count = len(df)
            df = df.dropna()
            print(f"📈 Clean records: {len(df)} (removed {initial_count - len(df)} null rows)")
            
            # Use bulk insert for better performance
            df_clean = df[['store_id', 'timestamp_utc', 'status']].copy()
            
            # Insert in batches of 5000 for memory efficiency
            batch_size = 5000
            total_inserted = 0
            
            for i in range(0, len(df_clean), batch_size):
                batch = df_clean[i:i+batch_size]
                batch.to_sql('store_status', engine, if_exists='append', index=False)
                total_inserted += len(batch)
                print(f"✅ Inserted batch {i//batch_size + 1}: {total_inserted}/{len(df_clean)} records")
            
            print(f"🎉 Store status ingestion completed: {total_inserted} records")
            return total_inserted
            
        except Exception as e:
            print(f"❌ Error processing store status: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def ingest_business_hours(self, db: Session):
        """Ingest business hours data from CSV"""
        print("🕐 Processing business hours data...")
        
        possible_names = [
            "menu_hours.csv",
            "Menu hours.csv",
            "business_hours.csv",
            "store_hours.csv"
        ]
        
        file_path = self.find_csv_file(possible_names)
        if not file_path:
            print(f"⚠️  Business hours file not found. Assuming 24/7 for all stores.")
            print(f"   Tried: {possible_names}")
            return 0
        
        try:
            df = pd.read_csv(file_path)
            print(f"📊 Loaded {len(df)} business hours records")
            print(f"📋 Columns: {list(df.columns)}")
            
            # Handle column name variations
            if 'day' not in df.columns and 'dayOfWeek' in df.columns:
                df['day'] = df['dayOfWeek']
            
            # Process in smaller batches for time parsing
            processed_records = []
            error_count = 0
            
            for _, row in df.iterrows():
                try:
                    # Parse time strings
                    start_time = datetime.strptime(str(row['start_time_local']), '%H:%M:%S').time()
                    end_time = datetime.strptime(str(row['end_time_local']), '%H:%M:%S').time()
                    
                    processed_records.append({
                        'store_id': str(row['store_id']),
                        'day_of_week': int(row['day']),
                        'start_time_local': start_time,
                        'end_time_local': end_time
                    })
                    
                    if len(processed_records) % 1000 == 0:
                        print(f"⏳ Processed {len(processed_records)} business hour records...")
                        
                except Exception as e:
                    error_count += 1
                    continue
            
            # Bulk insert
            if processed_records:
                # Convert time objects to strings for bulk insert
                for record in processed_records:
                    record['start_time_local'] = record['start_time_local'].strftime('%H:%M:%S')
                    record['end_time_local'] = record['end_time_local'].strftime('%H:%M:%S')
                
                df_processed = pd.DataFrame(processed_records)
                df_processed.to_sql('business_hours', engine, if_exists='append', index=False)
                
                print(f"✅ Business hours ingestion completed: {len(processed_records)} records")
                if error_count > 0:
                    print(f"⚠️  Skipped {error_count} invalid records")
                    
                return len(processed_records)
            
        except Exception as e:
            print(f"❌ Error processing business hours: {e}")
            return 0
    
    def ingest_timezones(self, db: Session):
        """Ingest timezone data from CSV"""
        print("🌍 Processing timezone data...")
        
        possible_names = [
            "bq-results-20230125-202210-1674678181880.csv",
            "store_timezones.csv",
            "timezones.csv"
        ]
        
        file_path = self.find_csv_file(possible_names)
        if not file_path:
            print(f"⚠️  Timezone file not found. Assuming America/Chicago for all stores.")
            print(f"   Tried: {possible_names}")
            return 0
        
        try:
            df = pd.read_csv(file_path)
            print(f"📊 Loaded {len(df)} timezone records")
            print(f"📋 Columns: {list(df.columns)}")
            
            # Clean data
            df['store_id'] = df['store_id'].astype(str)
            df = df.dropna()
            
            # Use bulk insert
            df_clean = df[['store_id', 'timezone_str']].copy()
            df_clean.to_sql('store_timezone', engine, if_exists='append', index=False)
            
            print(f"✅ Timezone ingestion completed: {len(df_clean)} records")
            return len(df_clean)
            
        except Exception as e:
            print(f"❌ Error processing timezones: {e}")
            return 0
    
    def initialize_database(self):
        """Initialize database and ingest all data"""
        print("🚀 Initializing Store Monitoring Database...")
        print("=" * 50)
        
        # Create tables
        print("🏗️  Creating database tables...")
        Base.metadata.create_all(bind=engine)
        
        # Download and extract data
        if not self.download_and_extract_data():
            print("❌ Failed to download/extract data")
            return False
        
        # Get database session
        db = next(get_db())
        
        try:
            # Clear existing data
            print("🧹 Clearing existing data...")
            deleted_status = db.query(StoreStatus).delete()
            deleted_hours = db.query(BusinessHours).delete()
            deleted_timezone = db.query(StoreTimezone).delete()
            db.commit()
            print(f"🗑️  Cleared: {deleted_status} status, {deleted_hours} hours, {deleted_timezone} timezone records")
            
            # Ingest new data
            print("\n📊 Starting data ingestion...")
            status_count = self.ingest_store_status(db)
            hours_count = self.ingest_business_hours(db)
            timezone_count = self.ingest_timezones(db)
            
            print("\n" + "=" * 50)
            print("🎉 DATA INGESTION SUMMARY:")
            print(f"   🏪 Store Status Records: {status_count:,}")
            print(f"   🕐 Business Hours Records: {hours_count:,}")
            print(f"   🌍 Timezone Records: {timezone_count:,}")
            
            if status_count > 0:
                print("✅ Database initialization completed successfully!")
                return True
            else:
                print("❌ No store status data loaded - reports will be empty")
                return False
            
        except Exception as e:
            print(f"❌ Error during database initialization: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            db.close()
