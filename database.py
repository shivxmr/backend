import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timezone  
import logging
import os
from typing import Generator
from contextlib import contextmanager

# Ensure logs directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database URL configuration with default fallback
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://shivxmr:shivam@localhost:5432/elt_db")

# Create SQLAlchemy engine with error handling and connection pooling
try:
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
    logger.info("Database engine created successfully")
except Exception as e:
    logger.error(f"Failed to create database engine: {str(e)}")
    raise

# Session configuration
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class MergedData(Base):
    """Model for storing the merged data from MTR and Payment reports."""
    __tablename__ = "merged_data"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(50), index=True, nullable=True)
    transaction_type = Column(String(50))
    payment_type = Column(String(50), nullable=True)
    description = Column(Text)
    invoice_amount = Column(Float, nullable=True)
    net_amount = Column(Float, nullable=True)
    payment_net_amount = Column(Float, nullable=True)
    shipment_invoice_amount = Column(Float, nullable=True)
    order_date = Column(DateTime, nullable=True)
    payment_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationship to ProcessedData
    processed_data = relationship("ProcessedData", back_populates="merged_data", uselist=False)

class ProcessedData(Base):
    """Model for storing the processed and categorized data."""
    __tablename__ = "processed_data"

    id = Column(Integer, primary_key=True, index=True)
    merged_data_id = Column(Integer, ForeignKey('merged_data.id'), unique=True)
    order_id = Column(String(50), index=True)
    category = Column(String(100), index=True)
    transaction_type = Column(String(50))
    payment_type = Column(String(50), nullable=True)
    invoice_amount = Column(Float, nullable=True)
    net_amount = Column(Float, nullable=True)
    payment_net_amount = Column(Float, nullable=True)
    shipment_invoice_amount = Column(Float, nullable=True)
    description = Column(Text, nullable=True)
    is_removal_order = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationship to MergedData
    merged_data = relationship("MergedData", back_populates="processed_data")
    
    # Relationship to ToleranceAnalysis
    tolerance_analysis = relationship("ToleranceAnalysis", back_populates="processed_data", uselist=False)

class EmptyOrderSummary(Base):
    """Model for storing summary of transactions with empty Order IDs."""
    __tablename__ = "empty_order_summary"

    id = Column(Integer, primary_key=True, index=True)
    description = Column(Text)
    total_net_amount = Column(Float)
    transaction_count = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class ToleranceAnalysis(Base):
    """Model for storing tolerance analysis results."""
    __tablename__ = "tolerance_analysis"

    id = Column(Integer, primary_key=True, index=True)
    processed_data_id = Column(Integer, ForeignKey('processed_data.id'), unique=True)
    order_id = Column(String(50), index=True)
    payment_net_amount = Column(Float)
    shipment_invoice_amount = Column(Float)
    tolerance_percentage = Column(Float)
    tolerance_threshold = Column(Float)
    tolerance_status = Column(String(50))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationship to ProcessedData
    processed_data = relationship("ProcessedData", back_populates="tolerance_analysis")

class ExemplarReport(Base):
    """Model for storing exemplar report data."""
    __tablename__ = "exemplar_report"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(50), index=True)
    transaction_type = Column(String(50))
    payment_type = Column(String(50))
    invoice_amount = Column(Float)
    net_amount = Column(Float)
    p_description = Column(String(500))
    order_date = Column(DateTime)
    payment_date = Column(DateTime)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

@contextmanager
def get_db() -> Generator:
    """Database session context manager with error handling."""
    db = SessionLocal()
    try:
        yield db
        logger.debug("Database session created successfully")
    except Exception as e:
        db.rollback()
        logger.error(f"Database session error: {str(e)}")
        raise
    finally:
        db.close()
        logger.debug("Database session closed")

def init_db() -> None:
    """Initialize database with error handling and logging."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

def check_db_connection() -> bool:
    """Check database connection health."""
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Database connection check successful")
        return True
    except Exception as e:
        logger.error(f"Database connection check failed: {str(e)}")
        return False
    
def insert_exemplar_data(file_path: str) -> None:
    """
    Insert data from exemplar report Excel file into the database.
    Handles null datetime values and includes error handling for individual rows.
    
    Args:
        file_path (str): Path to the exemplar report Excel file
    """
    try:
        # Read the Excel file
        logger.info(f"Reading exemplar report from: {file_path}")
        df = pd.read_excel(file_path)
        
        # Remove rows where all values are NaN
        df = df.dropna(how='all')
        logger.info(f"Processed {len(df)} rows from exemplar report")
        
        # Create database session
        with get_db() as db:
            # Insert each row into the database
            successful_inserts = 0
            failed_inserts = 0
            
            for _, row in df.iterrows():
                try:
                    # Handle datetime conversions with null checking
                    order_date = pd.to_datetime(row.get('Order Date'), errors='coerce')
                    payment_date = pd.to_datetime(row.get('Payment Date'), errors='coerce')
                    
                    # Convert NaT to None for SQL compatibility
                    order_date = None if pd.isna(order_date) else order_date
                    payment_date = None if pd.isna(payment_date) else payment_date
                    
                    # Handle potential null values for numeric fields
                    # invoice_amount = float(row.get('Invoice Amount').replace(',', '')) if pd.notnull(row.get('Invoice Amount')) else None
                    # net_amount = float(row.get('Net Amount').replace(',', '')) if pd.notnull(row.get('Net Amount')) else None
                    
                    invoice_amount = float(row.get('Invoice Amount').replace(',', '')) if isinstance(row.get('Invoice Amount'), str) and pd.notnull(row.get('Invoice Amount')) else None
                    net_amount = float(row.get('Net Amount').replace(',', '')) if isinstance(row.get('Net Amount'), str) and pd.notnull(row.get('Net Amount')) else None
                    

                    exemplar_entry = ExemplarReport(
                        order_id=str(row.get('Order Id')) if pd.notnull(row.get('Order Id')) else None,
                        transaction_type=str(row.get('Transaction Type')) if pd.notnull(row.get('Transaction Type')) else None,
                        payment_type=str(row.get('Payment Type')) if pd.notnull(row.get('Payment Type')) else None,
                        invoice_amount=invoice_amount,
                        net_amount=net_amount,
                        p_description=str(row.get('P Description')) if pd.notnull(row.get('P Description')) else None,
                        order_date=order_date,
                        payment_date=payment_date
                    )
                    
                    db.add(exemplar_entry)
                    successful_inserts += 1
                    
                    # Commit in batches of 500 to prevent memory issues
                    if successful_inserts % 500 == 0:
                        db.flush()
                        db.commit()
                        logger.info(f"Committed batch of {successful_inserts} records")
                        
                except Exception as row_error:
                    failed_inserts += 1
                    logger.error(f"Error inserting row: {row_error}")
                    db.rollback()  # Rollback the failed row
                    continue
            
            # Final commit for any remaining records
            try:
                db.commit()
                logger.info(f"Successfully inserted {successful_inserts} records into database")
                if failed_inserts > 0:
                    logger.warning(f"Failed to insert {failed_inserts} records")
            except Exception as commit_error:
                logger.error(f"Error during final commit: {commit_error}")
                db.rollback()
                raise
            
    except Exception as e:
        logger.error(f"Failed to insert exemplar report data: {str(e)}")
        raise

if __name__ == "__main__":
    init_db()