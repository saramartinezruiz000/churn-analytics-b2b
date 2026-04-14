import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'etl_pipeline.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_data(raw_dir):
    logging.info("Starting extraction process...")
    try:
        customers = pd.read_csv(os.path.join(raw_dir, 'customers_raw.csv'))
        subscriptions = pd.read_csv(os.path.join(raw_dir, 'subscriptions_raw.csv'))
        tickets = pd.read_csv(os.path.join(raw_dir, 'support_tickets_raw.csv'))
        logging.info("Extraction completed successfully.")
        return customers, subscriptions, tickets
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def transform_customers(df):
    logging.info("Transforming customers data...")
    df = df.copy()
    
    # Clean string columns
    df['industry'] = df['industry'].fillna('Unknown').str.title()
    df['country'] = df['country'].str.strip().str.upper()
    df['country'] = df['country'].replace({'USA': 'US'})
    
    # Handle missing numericals
    median_emp = df['employee_count'].median()
    df['employee_count'] = df['employee_count'].fillna(median_emp).astype(int)
    
    # Standardize dates
    df['signup_date'] = pd.to_datetime(df['signup_date']).dt.date
    return df

def transform_subscriptions(df):
    logging.info("Transforming subscriptions data...")
    df = df.copy()
    
    # Standardize plans
    df['plan_tier'] = df['plan_tier'].str.strip().str.capitalize()
    
    # Fill missing MRR
    avg_mrr_by_plan = df.groupby('plan_tier')['mrr_amount'].transform('mean')
    df['mrr_amount'] = df['mrr_amount'].fillna(avg_mrr_by_plan).round(2)
    
    # Standardize Dates
    df['cancellation_date'] = pd.to_datetime(df['cancellation_date'], errors='coerce').dt.date
    
    return df

def transform_tickets(df):
    logging.info("Transforming support tickets data...")
    df = df.copy()
    
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    # Fill missing resolution times with median of the issue type
    median_res = df.groupby('issue_type')['resolution_time_hrs'].transform('median')
    df['resolution_time_hrs'] = df['resolution_time_hrs'].fillna(median_res).round(1)
    
    return df

def load_data(customers_clean, subscriptions_clean, tickets_clean, processed_dir):
    logging.info("Starting load process into Data Warehouse (SQLite)...")
    db_path = os.path.join(processed_dir, 'churn_analytics_dw.db')
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Load tables
        customers_clean.to_sql('dim_customers', conn, if_exists='replace', index=False)
        subscriptions_clean.to_sql('fact_subscriptions', conn, if_exists='replace', index=False)
        tickets_clean.to_sql('fact_tickets', conn, if_exists='replace', index=False)
        
        # Create a combined analytical view
        conn.execute("""
            CREATE VIEW IF NOT EXISTS vw_churn_analysis AS
            SELECT 
                c.company_id, c.company_name, c.industry, c.country, c.signup_date,
                s.sub_id, s.plan_tier, s.status, s.mrr_amount, s.cancellation_date,
                COUNT(t.ticket_id) as total_tickets,
                AVG(t.resolution_time_hrs) as avg_resolution_time
            FROM dim_customers c
            LEFT JOIN fact_subscriptions s ON c.company_id = s.company_id
            LEFT JOIN fact_tickets t ON c.company_id = t.company_id
            GROUP BY 1,2,3,4,5,6,7,8,9,10
        """)
        
        conn.close()
        logging.info(f"Load completed successfully. Database saved at: {db_path}")
    except Exception as e:
        logging.error(f"Error during load: {e}")
        raise

def run_pipeline():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    
    logging.info("--- STARTING ETL PIPELINE ---")
    
    # Extract
    raw_cust, raw_subs, raw_ticks = extract_data(raw_dir)
    
    # Transform
    clean_cust = transform_customers(raw_cust)
    clean_subs = transform_subscriptions(raw_subs)
    clean_ticks = transform_tickets(raw_ticks)
    
    # Load
    load_data(clean_cust, clean_subs, clean_ticks, processed_dir)
    
    logging.info("--- ETL PIPELINE COMPLETED SUCCESSFULLY ---")
    print("ETL Pipeline execution completed. Check 'logs/etl_pipeline.log' for details.")

if __name__ == "__main__":
    run_pipeline()
