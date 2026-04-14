import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
import os

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

def generate_dirty_b2b_data(num_customers=2500):
    print("Generating simulated B2B SaaS data...")
    
    # 1. Customers Data
    industries = ['Technology', 'Healthcare', 'Finance', 'Education', 'Manufacturing', 'Retail', None]
    countries = ['US', 'CA', 'UK', 'MX', 'CO', 'ES', 'us', 'usa', ' MEXICO'] # Messy country names
    
    customers = []
    for _ in range(num_customers):
        company_id = str(uuid.uuid4())[:8].upper()
        customer = {
            'company_id': company_id,
            'company_name': f"Corp_{company_id}",
            'industry': random.choice(industries),
            'country': random.choice(countries),
            'employee_count': int(abs(np.random.normal(150, 200))) if random.random() > 0.05 else None, # 5% nulls
            'signup_date': (datetime(2021, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d')
        }
        customers.append(customer)
        
    df_customers = pd.DataFrame(customers)
    
    # 2. Subscriptions Data
    plans = ['Basic', 'Pro', 'Enterprise', '  Basic ', 'pro '] # Messy plans
    base_mrr = {'Basic': 199, 'Pro': 499, 'Enterprise': 1299}
    
    subscriptions = []
    for cust in customers:
        if random.random() > 0.95: continue # 5% of customers don't have active sub data (data inconsistency)
        
        plan = random.choice(plans)
        clean_plan = plan.strip().capitalize()
        mrr = base_mrr.get(clean_plan, 199) * random.uniform(0.9, 1.2) # Discounts/addons
        
        status = 'Active'
        end_date = None
        
        # Simulate Churn (around 15-20% churn rate)
        if random.random() < 0.18:
            status = 'Churned'
            signup = datetime.strptime(cust['signup_date'], '%Y-%m-%d')
            end_date = (signup + timedelta(days=random.randint(30, 700))).strftime('%Y/%m/%d') # Different date format
        
        # Introduce some missing MRR values
        if random.random() < 0.03: mrr = np.nan
            
        sub = {
            'sub_id': f"SUB-{str(uuid.uuid4())[:6].upper()}",
            'company_id': cust['company_id'],
            'plan_tier': plan,
            'mrr_amount': mrr,
            'status': status,
            'cancellation_date': end_date
        }
        subscriptions.append(sub)
        
    df_subscriptions = pd.DataFrame(subscriptions)
    
    # 3. Support Tickets Data
    tickets = []
    ticket_types = ['Billing', 'Technical', 'Onboarding', 'Feature Request']
    
    # Generate 5000+ random tickets
    for _ in range(num_customers * 2):
        company_id = random.choice(customers)['company_id']
        created = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        
        tickets.append({
            'ticket_id': f"TCK-{random.randint(10000, 99999)}",
            'company_id': company_id,
            'issue_type': random.choice(ticket_types),
            'created_at': created.strftime('%Y-%m-%d %H:%M:%S'),
            'resolution_time_hrs': max(1, np.random.normal(24, 12)) if random.random() > 0.02 else None # Missing resolution time
        })
        
    df_tickets = pd.DataFrame(tickets)
    
    # Create directory if it doesn't exist
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)
    
    # Save as CSVs
    df_customers.to_csv(os.path.join(raw_dir, 'customers_raw.csv'), index=False)
    df_subscriptions.to_csv(os.path.join(raw_dir, 'subscriptions_raw.csv'), index=False)
    df_tickets.to_csv(os.path.join(raw_dir, 'support_tickets_raw.csv'), index=False)
    
    print(f"Generated {len(df_customers)} customers")
    print(f"Generated {len(df_subscriptions)} subscriptions")
    print(f"Generated {len(df_tickets)} support tickets")
    print("Files saved to 'data/raw/' directory. They contain messy/dirty data by design for ETL processing.")

if __name__ == "__main__":
    generate_dirty_b2b_data()
