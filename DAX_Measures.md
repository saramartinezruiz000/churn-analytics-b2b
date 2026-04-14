# DAX Measures for Churn Analytics Dashboard

This file documents the advanced DAX measures created in Power BI for the Churn Analytics Dashboard. This demonstrates the ability to calculate Time Intelligence, Cohorts, and key SaaS metrics.

## 1. MRR (Monthly Recurring Revenue)
Calculates the current active MRR.
```dax
Total MRR = 
CALCULATE(
    SUM('vw_churn_analysis'[mrr_amount]),
    'vw_churn_analysis'[status] = "Active"
)
```

## 2. Customer Churn Rate
Percentage of customers who canceled their subscription within the selected period.
```dax
Customer Churn Rate % = 
VAR TotalCustomers = COUNTROWS(FILTER('vw_churn_analysis', 'vw_churn_analysis'[status] = "Active" || 'vw_churn_analysis'[status] = "Churned"))
VAR ChurnedCustomers = COUNTROWS(FILTER('vw_churn_analysis', 'vw_churn_analysis'[status] = "Churned"))
RETURN 
    DIVIDE(ChurnedCustomers, TotalCustomers, 0)
```

## 3. MRR Churn (Revenue Lost)
Total revenue lost due to cancellations.
```dax
MRR Churn = 
CALCULATE(
    SUM('vw_churn_analysis'[mrr_amount]),
    'vw_churn_analysis'[status] = "Churned"
)
```

## 4. Average Resolution Time vs Churn correlation
A measure to identify if ticket resolution time impacts churn.
```dax
Avg Res Time (Churned Users) = 
CALCULATE(
    AVERAGE('vw_churn_analysis'[avg_resolution_time]),
    'vw_churn_analysis'[status] = "Churned"
)

Avg Res Time (Active Users) = 
CALCULATE(
    AVERAGE('vw_churn_analysis'[avg_resolution_time]),
    'vw_churn_analysis'[status] = "Active"
)
```

## 5. Year-to-Date (YTD) New MRR
Time Intelligence formula to track MRR growth over the year.
```dax
YTD New MRR = 
TOTALYTD(
    SUM('vw_churn_analysis'[mrr_amount]),
    'DateTable'[Date]
)
```
