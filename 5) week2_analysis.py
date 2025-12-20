# ===============================
# 1. IMPORT LIBRARIES
# ===============================
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from mlxtend.frequent_patterns import apriori, association_rules

# ===============================
# 2. CONNECT TO MYSQL
# ===============================
engine = create_engine(
    "mysql+pymysql://root:Divyani%40123Mysql@127.0.0.1:3306/retail_project1"
)

with engine.connect() as conn:
    print("CONNECTED TO MYSQL")

# ===============================
# 3. PULL DATA FROM SQL
# ===============================
query = """
SELECT customer_id,
       invoice_date,
       product_name,
       revenue
FROM customer_360
"""

df = pd.read_sql(query, engine)
print("\nRAW DATA:")
print(df.head())

# ===============================
# 4. DATA CLEANING
# ===============================
df['invoice_date'] = pd.to_datetime(df['invoice_date'])
df = df.dropna(subset=['customer_id', 'invoice_date', 'product_name', 'revenue'])

# ===============================
# 5. SNAPSHOT DATE
# ===============================
snapshot_date = df['invoice_date'].max() + pd.Timedelta(days=1)

# ===============================
# 6. RFM CALCULATION
# ===============================
rfm = df.groupby('customer_id').agg(
    recency=('invoice_date', lambda x: (snapshot_date - x.max()).days),
    frequency=('invoice_date', 'count'),     # no invoice_no needed
    monetary=('revenue', 'sum')
).reset_index()

print("\nRFM TABLE:")
print(rfm.head())

# ===============================
# 7. FIX DATA TYPES
# ===============================
rfm['recency'] = rfm['recency'].astype(int)
rfm['frequency'] = rfm['frequency'].astype(int)
rfm['monetary'] = rfm['monetary'].astype(float)

# ===============================
# 8. RFM SCORING (NO qcut ERROR)
# ===============================
rfm['R_score'] = pd.qcut(
    rfm['recency'].rank(method='first'),
    5,
    labels=[5,4,3,2,1]
)

rfm['F_score'] = pd.qcut(
    rfm['frequency'].rank(method='first'),
    5,
    labels=[1,2,3,4,5]
)

rfm['M_score'] = pd.qcut(
    rfm['monetary'].rank(method='first'),
    5,
    labels=[1,2,3,4,5]
)

# ===============================
# 9. CUSTOMER SEGMENTATION
# ===============================
def segment_customer(row):
    if row['R_score'] >= 4 and row['F_score'] >= 4 and row['M_score'] >= 4:
        return 'Champions'
    elif row['F_score'] >= 4:
        return 'Loyal Customers'
    elif row['R_score'] >= 4:
        return 'New Customers'
    elif row['M_score'] >= 3:
        return 'Potential Loyalist'
    else:
        return 'Hibernating'

rfm['Segment'] = rfm.apply(segment_customer, axis=1)

# ===============================
# 10. SEGMENT VALIDATION (REQUIRED)
# ===============================
print("\nCUSTOMERS PER SEGMENT:")
print(rfm['Segment'].value_counts())

print("\nAVERAGE SPEND PER SEGMENT:")
print(
    rfm.groupby('Segment')['monetary']
       .mean()
       .sort_values(ascending=False)
)

# Save RFM output
rfm.to_csv("rfm_segments.csv", index=False)
print("\nRFM FILE SAVED")

# ===============================
# 11. ASSOCIATION RULE MINING
# ===============================

# Create transaction_id (customer + date)
df['transaction_id'] = (
    df['customer_id'].astype(str) + "_" +
    df['invoice_date'].dt.date.astype(str)
)

# Create basket
basket = (
    df.groupby(['transaction_id', 'product_name'])
      .size()
      .unstack(fill_value=0)
      .astype(bool)
)

# Debug checks
print("\nAVERAGE ITEMS PER TRANSACTION:")
print(basket.sum(axis=1).mean())

print("\nTRANSACTIONS WITH >1 ITEM:"),
((basket.sum(axis=1) > 1).sum())

# ===============================
# 12. APRIORI
# ===============================
frequent_itemsets = apriori(
    basket,
    min_support=0.001,   # LOW support for small datasets
    use_colnames=True
)

print("\nFREQUENT ITEMSETS:")
print(frequent_itemsets.head())

# ===============================
# 13. ASSOCIATION RULES
# ===============================
rules = association_rules(
    frequent_itemsets,
    metric="confidence",
    min_threshold=0.1
)

print("\nASSOCIATION RULES:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']])

print("Unique products:", basket.shape[1])
print("Total transactions:", basket.shape[0])
