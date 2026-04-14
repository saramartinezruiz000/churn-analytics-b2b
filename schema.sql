-- Data Warehouse Schema for Churn Analytics Project
-- Used to recreate the SQLite/PostgreSQL structure in production.

-- 1. Dimension Table: Customers
CREATE TABLE dim_customers (
    company_id VARCHAR(50) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    industry VARCHAR(100),
    country VARCHAR(10),
    employee_count INTEGER,
    signup_date DATE
);

-- 2. Fact Table: Subscriptions
CREATE TABLE fact_subscriptions (
    sub_id VARCHAR(50) PRIMARY KEY,
    company_id VARCHAR(50) REFERENCES dim_customers(company_id),
    plan_tier VARCHAR(50),
    mrr_amount DECIMAL(10,2),
    status VARCHAR(20),
    cancellation_date DATE
);

-- 3. Fact Table: Support Tickets
CREATE TABLE fact_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    company_id VARCHAR(50) REFERENCES dim_customers(company_id),
    issue_type VARCHAR(100),
    created_at TIMESTAMP,
    resolution_time_hrs DECIMAL(10,1)
);

-- 4. Analytical View: Churn Overview
-- This view feeds directly into Power BI to simplify the Data Model.
CREATE VIEW vw_churn_analysis AS
SELECT 
    c.company_id, 
    c.company_name, 
    c.industry, 
    c.country, 
    c.signup_date,
    s.sub_id, 
    s.plan_tier, 
    s.status, 
    s.mrr_amount, 
    s.cancellation_date,
    COUNT(t.ticket_id) as total_tickets,
    AVG(t.resolution_time_hrs) as avg_resolution_time
FROM dim_customers c
LEFT JOIN fact_subscriptions s ON c.company_id = s.company_id
LEFT JOIN fact_tickets t ON c.company_id = t.company_id
GROUP BY 
    c.company_id, c.company_name, c.industry, c.country, c.signup_date,
    s.sub_id, s.plan_tier, s.status, s.mrr_amount, s.cancellation_date;
