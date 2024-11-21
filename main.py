from fastapi import FastAPI, UploadFile, HTTPException, Depends
import pandas as pd
import io
import logging
import os
from datetime import datetime
from pathlib import Path
from processing import categorize_transactions, store_merged_data, store_processed_data, process_empty_orders, perform_tolerance_analysis, process_exemplar_report
from database import MergedData, ProcessedData, EmptyOrderSummary, ToleranceAnalysis, get_db, insert_exemplar_data
from sqlalchemy.orm import Session

# Create necessary directories
logs_dir = Path("logs")
output_dir = Path("output")

try:
    logs_dir.mkdir(exist_ok=True)
    logger_setup_msg = f"Logs directory created/verified at: {logs_dir.absolute()}"
except Exception as e:
    logger_setup_msg = f"Error creating logs directory: {str(e)}"
    raise

try:
    output_dir.mkdir(exist_ok=True)
    output_setup_msg = f"Output directory created/verified at: {output_dir.absolute()}"
except Exception as e:
    output_setup_msg = f"Error creating output directory: {str(e)}"
    raise

# Set up logging
log_filename = f"transformation-log-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
log_filepath = logs_dir / log_filename

print(f"Setting up logging to: {log_filepath.absolute()}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

def find_column(df, possible_names):
    """Find a column regardless of case"""
    logger.debug(f"Searching for columns with possible names: {possible_names}")
    for col in df.columns:
        if col.lower() in [name.lower() for name in possible_names]:
            logger.info(f"Found matching column: {col}")
            return col
    logger.warning(f"No matching column found for names: {possible_names}")
    return None

def clean_string_values(df):
    """Clean string values in the DataFrame by stripping whitespace and newlines"""
    logger.info("Starting string value cleaning")
    original_shape = df.shape
    
    for column in df.select_dtypes(include=['object']).columns:
        logger.debug(f"Cleaning string values in column: {column}")
        df[column] = df[column].str.strip()
        
    logger.info(f"String cleaning completed. DataFrame shape maintained: {original_shape == df.shape}")
    return df

def case_insensitive_replace(series, mapping):
    """Replace values in a case-insensitive manner"""
    logger.debug(f"Starting case-insensitive replacement with mapping: {mapping}")
    series_lower = series.str.lower()
    mapping_lower = {k.lower(): v for k, v in mapping.items()}
    new_series = series.copy()
    
    for old_val_lower, new_val in mapping_lower.items():
        mask = series_lower == old_val_lower
        replacement_count = mask.sum()
        new_series[mask] = new_val
        logger.debug(f"Replaced {replacement_count} occurrences of '{old_val_lower}' with '{new_val}'")
    
    return new_series

def process_payment_report(payment_data):
    """Process payment report data"""
    logger.info("Starting payment report processing")
    try:
        df = payment_data.copy()
        logger.info(f"Initial payment data shape: {df.shape}")
        
        df = clean_string_values(df)
        logger.debug("String values cleaned")
        
        # Find columns
        type_col = find_column(df, ['type', 'Type', 'TYPE'])
        desc_col = find_column(df, ['description', 'Description', 'DESCRIPTION'])
        
        if not type_col or not desc_col:
            error_msg = "Required columns not found in payment report"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Found required columns - Type: {type_col}, Description: {desc_col}")
        
        # Remove transfer rows
        initial_rows = len(df)
        df = df[~df[type_col].str.lower().str.contains('transfer', na=False)]
        removed_rows = initial_rows - len(df)
        logger.info(f"Removed {removed_rows} transfer rows")
        
        # Define mappings
        type_mapping = {
            'Refund': 'Return',
            'Adjustment': 'Order',
            'FBA Inventory Fee': 'Order',
            'Fulfilment Fee Refund': 'Order',
            'Service Fee': 'Order'
        }
        
        desc_mapping = {
            'Adjustment': 'Order',
            'FBA Inventory Fee': 'Order',
            'Fulfillment Fee Refund': 'Order',
            'Service Fee': 'Order',
            'FBA Inventory Reimbursement - Customer Service Issue': 'Order'
        }
        
        logger.debug("Applying type and description mappings")
        # Apply mappings
        df[type_col] = case_insensitive_replace(df[type_col], type_mapping)
        df[desc_col] = case_insensitive_replace(df[desc_col], desc_mapping)
        
        # Handle refunds
        refund_count = df[type_col].str.lower() == 'refund'
        df.loc[refund_count, type_col] = 'Return'
        logger.info(f"Converted {refund_count.sum()} refund entries to Return")
        
        # Rename columns
        df = df.rename(columns={type_col: 'Payment Type'})
        logger.debug("Renamed type column to 'Payment Type'")
        
        # Add Transaction Type
        df['Transaction Type'] = 'Payment'
        logger.info("Added Transaction Type column")
        
        logger.info(f"Payment report processing completed. Final shape: {df.shape}")
        return df
    
    except Exception as e:
        logger.error(f"Error processing payment report: {str(e)}", exc_info=True)
        raise

def process_mtr_report(mtr_data):
    """Process MTR report data"""
    logger.info("Starting MTR report processing")
    try:
        df = mtr_data.copy()
        logger.info(f"Initial MTR data shape: {df.shape}")
        
        # Remove Cancel transactions
        initial_rows = len(df)
        df = df[df["Transaction Type"] != "Cancel"]
        removed_rows = initial_rows - len(df)
        logger.info(f"Removed {removed_rows} Cancel transactions")
        
        # Replace transaction types
        transaction_mapping = {
            "Refund": "Return",
            "FreeReplacement": "Return"
        }
        initial_types = df["Transaction Type"].value_counts()
        df["Transaction Type"] = df["Transaction Type"].replace(transaction_mapping)
        final_types = df["Transaction Type"].value_counts()
        
        logger.info("Transaction type changes:")
        for type_name in transaction_mapping:
            if type_name in initial_types:
                logger.info(f"Converted {initial_types.get(type_name, 0)} {type_name} entries to Return")
        
        logger.info(f"MTR report processing completed. Final shape: {df.shape}")
        return df
    
    except Exception as e:
        logger.error(f"Error processing MTR report: {str(e)}", exc_info=True)
        raise

def create_exemplar_report(mtr_df, payment_df):
    """Create exemplar report from processed MTR and payment reports"""
    logger.info("Starting exemplar report creation")
    try:
        logger.info(f"Input shapes - MTR: {mtr_df.shape}, Payment: {payment_df.shape}")
        
        # Prepare merged data
        exemplar_data = {
            'Order Id': pd.concat([
                mtr_df['Order Id'],
                payment_df['order id']
            ], ignore_index=True),
            'Transaction Type': pd.concat([
                mtr_df['Transaction Type'],
                payment_df['Transaction Type']
            ], ignore_index=True),
            'Payment Type': pd.concat([
                pd.Series([None] * len(mtr_df)),
                payment_df['Payment Type']
            ], ignore_index=True),
            'Invoice Amount': pd.concat([
                mtr_df['Invoice Amount'],
                pd.Series([None] * len(payment_df))
            ], ignore_index=True),
            'Net Amount': pd.concat([
                pd.Series([None] * len(mtr_df)),
                payment_df['total']
            ], ignore_index=True),
            'P Description': pd.concat([
                mtr_df['Item Description'],
                payment_df['description']
            ], ignore_index=True),
            'Order Date': pd.concat([
                mtr_df['Order Date'],
                pd.Series([None] * len(payment_df))
            ], ignore_index=True),
            'Payment Date': pd.concat([
                pd.Series([None] * len(mtr_df)),
                payment_df['date/time']
            ], ignore_index=True)
        }
        
        logger.debug("Data concatenation completed")
        
        # Create DataFrame and add spacing
        exemplar_df = pd.DataFrame(exemplar_data)
        logger.info(f"Initial exemplar DataFrame shape: {exemplar_df.shape}")
        
        empty_rows = pd.DataFrame([None] * len(exemplar_df.columns)).T * 5
        exemplar_with_gaps = pd.concat([
            exemplar_df.iloc[:len(mtr_df)],
            empty_rows,
            exemplar_df.iloc[len(mtr_df):]
        ], ignore_index=True)
        
        logger.info(f"Final exemplar report shape with gaps: {exemplar_with_gaps.shape}")
        return exemplar_with_gaps
    
    except Exception as e:
        logger.error(f"Error creating exemplar report: {str(e)}", exc_info=True)
        raise


@app.post("/upload/")
async def upload_files(payment_report: UploadFile, mtr_report: UploadFile):
    """Process uploaded files and create exemplar report"""
    logger.info(f"Starting file processing - Payment: {payment_report.filename}, MTR: {mtr_report.filename}")
    try:
        # Read payment report
        payment_data = pd.read_csv(io.BytesIO(await payment_report.read()))
        logger.info(f"Payment report loaded successfully: {len(payment_data)} rows")
        
        # Read MTR report
        mtr_data = pd.read_excel(io.BytesIO(await mtr_report.read()))
        logger.info(f"MTR report loaded successfully: {len(mtr_data)} rows")
        
        # Process reports
        logger.info("Starting report transformations")
        processed_payment = process_payment_report(payment_data)
        processed_mtr = process_mtr_report(mtr_data)
        
        # Save transformed reports
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # Save reports with full paths
        payment_output_path = str(output_dir / "transformed_payment_report.csv")
        mtr_output_path = str(output_dir / "transformed_mtr_report.xlsx")
        exemplar_output_path = str(output_dir / "exemplar_report.xlsx")
        
        try:
            processed_payment.to_csv(payment_output_path, index=False)
            logger.info(f"Payment report saved successfully at: {payment_output_path}")
        except Exception as e:
            logger.error(f"Error saving payment report: {str(e)}", exc_info=True)
            raise
            
        try:
            processed_mtr.to_excel(mtr_output_path, index=False)
            logger.info(f"MTR report saved successfully at: {mtr_output_path}")
        except Exception as e:
            logger.error(f"Error saving MTR report: {str(e)}", exc_info=True)
            raise
            
        # Create and save exemplar report
        exemplar_report = create_exemplar_report(processed_mtr, processed_payment)
        
        try:
            exemplar_report.to_excel(exemplar_output_path, index=False)
            logger.info(f"Exemplar report saved successfully at: {exemplar_output_path}")
        except Exception as e:
            logger.error(f"Error saving exemplar report: {str(e)}", exc_info=True)
            raise
        logger.info("Exemplar report created and saved successfully")

        created_files = [
            "transformed_payment_report.csv",
            "transformed_mtr_report.xlsx",
            "exemplar_report.xlsx"
        ]

        insert_exemplar_data(exemplar_output_path)
        
        logger.info("File processing completed successfully")
        return {
            "message": "Processing completed successfully",
            "files_created": created_files
        }
        
    except Exception as e:
        error_msg = f"Error processing files: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=400, detail=error_msg)

if __name__ == "__main__":
    logger.info("Starting FastAPI application")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)