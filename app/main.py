from fastapi import FastAPI
from fastapi.responses import FileResponse
from app.api.endpoints import router
from app.services.data_ingestion import DataIngestionService
from app.database import Base, engine
import uvicorn
import os

app = FastAPI(title="Store Monitoring API", version="1.0.0")

# Include API routes
app.include_router(router, prefix="")

@app.on_event("startup")
async def startup_event():
    """Initialize database and ingest data on startup"""
    try:
        data_service = DataIngestionService()
        data_service.initialize_database()
        print("Application started successfully!")
    except Exception as e:
        print(f"Error during startup: {e}")

# @app.get("/")
# async def root():
#     return {"message": "Store Monitoring API is running"}
@app.get("/")
async def root():
    file_path = os.path.join(os.path.dirname(__file__), "test.html")
    return FileResponse(file_path)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
