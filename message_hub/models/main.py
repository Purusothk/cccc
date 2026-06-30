from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api import message_hub_api, deal_api, pipeline_api
from .core.db import trade_engine, message_engine
from .models import fxtr014_model, deal_model, event_model, pipeline_model

# Create database tables
fxtr014_model.Base.metadata.create_all(bind=message_engine)
pipeline_model.Base.metadata.create_all(bind=message_engine)
event_model.Base.metadata.create_all(bind=message_engine)
deal_model.Base.metadata.create_all(bind=trade_engine)

# Initialize FastAPI app
app = FastAPI(
    title="CrossBorder FX - Message Hub",
    description="Message Hub for FX Trade Processing and FXTR014 Generation",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def start_background_scheduler():
    import threading
    import time
    import logging
    from datetime import date, timedelta
    from app.message_hub.services.message_hub_service import MessageHubService
    from app.message_hub.core.db import MessageSessionLocal

    logger = logging.getLogger("scheduler")

    def run_scheduler_loop():
        time.sleep(5)
        logger.info("Background T+2 scheduler loop started.")
        while True:
            try:
                db = MessageSessionLocal()
                try:
                    service = MessageHubService(db)
                    simulated_date = date.today() + timedelta(days=2)
                    service.run_pain001_scheduler(as_of=simulated_date)
                finally:
                    db.close()
            except Exception as e:
                logger.error(f"Error in background scheduler: {str(e)}")
            time.sleep(5)

    thread = threading.Thread(target=run_scheduler_loop, daemon=True)
    thread.start()


# Include routers
app.include_router(message_hub_api.router)
if hasattr(deal_api, 'router'):
    app.include_router(deal_api.router)
if hasattr(pipeline_api, 'router'):
    app.include_router(pipeline_api.router)


@app.get("/", tags=["Root"])
def root():
    """Root endpoint"""
    return {
        "message": "CrossBorder FX - Message Hub API",
        "version": "1.0.0",
        "endpoints": {
            "message_hub": "/api/v1/message-hub",
            "docs": "/docs",
            "health": "/api/v1/message-hub/health"
        }
    }


@app.get("/api/v1/health", tags=["Health"])
def health():
    """API health check"""
    return {
        "status": "healthy",
        "service": "Message Hub API",
        "timestamp": __import__('datetime').datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
