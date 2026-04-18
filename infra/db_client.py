from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

class SignalLog(Base):
    __tablename__ = 'signal_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    symbol = Column(String(20), nullable=False)
    action = Column(String(10), nullable=False)  # BUY, SELL
    quantity = Column(Float, nullable=False)
    
    # Breakout Microstructure Metrics
    volume_shock = Column(Float, nullable=True)
    ofi_score = Column(Float, nullable=True)
    
    # Lead-Lag & LLM Analysis
    lead_lag_reason = Column(String(255), nullable=True)
    llm_sentiment = Column(Float, nullable=True)
    llm_theme = Column(String(100), nullable=True)
    confidence = Column(Float, nullable=True)
    
    # Full raw reason payload for debugging
    raw_reason = Column(String(500), nullable=True)

class DBManager:
    def __init__(self):
        # Default string for local docker-compose setup
        db_url = os.getenv("DB_URL", "postgresql://quant_user:quant_pass@postgres:5432/quant_db")
        self.engine = create_engine(db_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables if they don't exist
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables verified/created successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to DB and create tables: {e}")

    def log_signal(self, signal_data: dict, breakout_metrics: dict = None, llm_data: dict = None):
        """
        Save the generated execution signal into the PostgreSQL database.
        """
        session = self.SessionLocal()
        try:
            log_entry = SignalLog(
                symbol=signal_data.get('symbol', 'UNKNOWN'),
                action=signal_data.get('action', 'UNKNOWN'),
                quantity=signal_data.get('quantity', 0.0),
                raw_reason=signal_data.get('reason', ''),
                confidence=signal_data.get('confidence', 0.0)
            )

            # Populate Breakout Metrics if passed
            if breakout_metrics:
                log_entry.volume_shock = breakout_metrics.get('volume_ratio')
                log_entry.ofi_score = breakout_metrics.get('ofi')

            # Populate LLM and Theme data if passed
            if llm_data:
                log_entry.llm_sentiment = llm_data.get('sentiment_score')
                log_entry.llm_theme = llm_data.get('theme_keyword')

            session.add(log_entry)
            session.commit()
            logger.info(f"Successfully logged signal for {log_entry.symbol} to database.")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to log signal to database: {e}")
        finally:
            session.close()
