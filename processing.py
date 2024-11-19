import os
from sqlalchemy.orm import Session
from database import MergedData, ProcessedData, EmptyOrderSummary, ToleranceAnalysis
import pandas as pd
from typing import Dict, List, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Constants
EXEMPLAR_REPORT_PATH = "output/exemplar_report.xlsx"

def process_exemplar_report(db: Session) -> None:
    """Process the exemplar report and store results in the database."""
    try:
        logger.info(f"Starting processing of exemplar report from: {EXEMPLAR_REPORT_PATH}")
        
        # Check if file exists
        if not os.path.exists(EXEMPLAR_REPORT_PATH):
            raise FileNotFoundError(f"Exemplar report not found at: {EXEMPLAR_REPORT_PATH}")
            
        # Read the Excel file
        df = pd.read_excel(EXEMPLAR_REPORT_PATH)
        
        # Process the data using existing functions
        categorized_df = categorize_transactions(df)
        
        # Store data and get mappings
        order_id_mapping = store_merged_data(df, db)
        processed_id_mapping = store_processed_data(categorized_df, order_id_mapping, db)
        
        # Process empty orders
        process_empty_orders(df, db)
        
        # Perform tolerance analysis
        perform_tolerance_analysis(categorized_df, processed_id_mapping, db)
        
        logger.info("Exemplar report processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing exemplar report: {str(e)}", exc_info=True)
        raise

def categorize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Categorize transactions based on specified rules."""
    try:
        logger.info("Starting transaction categorization")
        
        # Initialize category column
        df['category'] = 'Uncategorized'
        
        # Remove Order IDs (length exactly 10)
        df.loc[df['order_id'].str.len() == 10, 'category'] = 'Removal Order'
        
        # Apply categorization rules using conditions
        df.loc[
            (df['transaction_type'] == 'Return') & df['invoice_amount'].notna(), 'category'] = 'Return'
        df.loc[
            (df['transaction_type'] == 'Payment') & (df['net_amount'] < 0), 'category'] = 'Negative Payout'
        df.loc[
            df['order_id'].notna() & df['payment_net_amount'].notna() & df['shipment_invoice_amount'].notna(), 
            'category'] = 'Order & Payment Received'
        df.loc[
            df['order_id'].notna() & df['payment_net_amount'].notna() & df['shipment_invoice_amount'].isna(), 
            'category'] = 'Order Not Applicable but Payment Received'
        df.loc[
            df['order_id'].notna() & df['shipment_invoice_amount'].notna() & df['payment_net_amount'].isna(), 
            'category'] = 'Payment Pending'
        
        logger.info("Transaction categorization completed successfully")
        return df
    
    except Exception as e:
        logger.error(f"Error in categorize_transactions: {str(e)}", exc_info=True)
        raise

def store_merged_data(df: pd.DataFrame, db: Session) -> Dict[str, int]:
    """Store merged data in the database and return mapping of order_ids to record ids."""
    return store_data_in_db(df, db, MergedData)

def store_processed_data(df: pd.DataFrame, order_id_mapping: Dict[str, int], db: Session) -> Dict[str, int]:
    """Store processed and categorized data in the database."""
    return store_data_in_db(df, db, ProcessedData, order_id_mapping)

def store_data_in_db(df: pd.DataFrame, db: Session, model_class, order_id_mapping: Dict[str, int] = None) -> Dict[str, int]:
    """Generalized function to store data in the database."""
    order_id_mapping = order_id_mapping or {}
    logger.info(f"Storing {model_class.__name__} data")
    
    for _, row in df.iterrows():
        entry = model_class(**row.to_dict())
        db.add(entry)
        db.flush()
        
        if row.get('order_id'):
            order_id_mapping[row['order_id']] = entry.id
            
    db.commit()
    logger.info(f"Stored {len(df)} {model_class.__name__} records")
    return order_id_mapping

def process_empty_orders(df: pd.DataFrame, db: Session) -> None:
    """Process and store summary for transactions with empty Order IDs."""
    try:
        logger.info("Processing empty order transactions")
        
        empty_orders = df[df['order_id'].isna()]
        summary = (
            empty_orders.groupby('description')
            .agg({
                'net_amount': 'sum',
                'order_id': 'count'
            })
            .reset_index()
            .rename(columns={'order_id': 'transaction_count'})
        )
        
        for _, row in summary.iterrows():
            summary_entry = EmptyOrderSummary(
                description=row['description'],
                total_net_amount=row['net_amount'],
                transaction_count=row['transaction_count']
            )
            db.add(summary_entry)
        
        db.commit()
        logger.info(f"Stored {len(summary)} empty order summaries")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing empty orders: {str(e)}", exc_info=True)
        raise

def perform_tolerance_analysis(df: pd.DataFrame, processed_id_mapping: Dict[str, int], db: Session) -> None:
    """Perform and store tolerance analysis for applicable transactions."""
    try:
        logger.info("Performing tolerance analysis")
        
        mask = df['payment_net_amount'].notna() & df['shipment_invoice_amount'].notna()
        tolerance_df = df[mask].copy()
        
        for _, row in tolerance_df.iterrows():
            if row['shipment_invoice_amount'] != 0:
                percentage = (row['payment_net_amount'] / row['shipment_invoice_amount']) * 100
                threshold, status = calculate_tolerance(row['payment_net_amount'], percentage)
                
                analysis_entry = ToleranceAnalysis(
                    processed_data_id=processed_id_mapping.get(row['order_id']),
                    order_id=row['order_id'],
                    payment_net_amount=row['payment_net_amount'],
                    shipment_invoice_amount=row['shipment_invoice_amount'],
                    tolerance_percentage=percentage,
                    tolerance_threshold=threshold,
                    tolerance_status=status
                )
                db.add(analysis_entry)
        
        db.commit()
        logger.info(f"Stored tolerance analysis for {len(tolerance_df)} records")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error in tolerance analysis: {str(e)}", exc_info=True)
        raise

def calculate_tolerance(pna: float, percentage: float) -> Tuple[float, str]:
    """Calculate tolerance threshold and status based on payment net amount."""
    try:
        if 0 < pna <= 300:
            threshold = 50
        elif 300 < pna <= 500:
            threshold = 45
        elif 500 < pna <= 900:
            threshold = 43
        elif 900 < pna <= 1500:
            threshold = 38
        else:
            threshold = 30
            
        status = "Within Tolerance" if percentage >= threshold else "Tolerance Breached"
        return threshold, status
    
    except Exception as e:
        logger.error(f"Error in calculate_tolerance: {str(e)}", exc_info=True)
        raise



# @app.get("/data/category/{category_name}")
# async def get_data_by_category(category_name: str, db: Session = Depends(get_db)):
#     logger.info(f"Endpoint '/data/category/{category_name}' called.")
#     try:
#         with db as session:  # Use the session within a context
#             data = session.query(ProcessedData).filter(ProcessedData.category == category_name).all()
#             logger.info(f"Retrieved {len(data)} records for category '{category_name}'.")
#             return {"data": [record.as_dict() for record in data]}
#     except Exception as e:
#         logger.error(f"Error in '/data/category/{category_name}': {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/data/empty_order_summary/")
# async def get_empty_order_summary(db: Session = Depends(get_db)):
#     logger.info("Endpoint '/data/empty_order_summary/' called.")
#     try:
#         with db as session:
#             summaries = session.query(EmptyOrderSummary).all()
#             logger.info(f"Retrieved {len(summaries)} empty order summaries.")
#             return {"summaries": [summary.as_dict() for summary in summaries]}
#     except Exception as e:
#         logger.error(f"Error in '/data/empty_order_summary/': {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/tolerance-analysis/")
# async def perform_tolerance_analysis_endpoint(db: Session = Depends(get_db)):
#     logger.info("Endpoint '/tolerance-analysis/' (POST) called.")
#     try:
#         with db as session:
#             df = pd.read_sql(session.query(MergedData).statement, session.bind)
#             logger.info("MergedData successfully loaded into DataFrame.")

#             order_id_mapping = {rec.order_id: rec.id for rec in session.query(ProcessedData).all()}
#             perform_tolerance_analysis(df, order_id_mapping, session)

#             logger.info("Tolerance analysis performed and results stored.")
#             return {"message": "Tolerance analysis performed and results stored"}
#     except Exception as e:
#         logger.error(f"Error in '/tolerance-analysis/': {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/tolerance-analysis/")
# async def get_tolerance_analysis(db: Session = Depends(get_db)):
#     logger.info("Endpoint '/tolerance-analysis/' (GET) called.")
#     try:
#         with db as session:
#             results = session.query(ToleranceAnalysis).all()
#             logger.info(f"Retrieved {len(results)} tolerance analysis records.")
#             return {"analysis_results": [result.as_dict() for result in results]}
#     except Exception as e:
#         logger.error(f"Error in '/tolerance-analysis/': {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e))
