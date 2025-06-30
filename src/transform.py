# trasform.py
from datetime import datetime
import uuid
import logging
import pandas as pd
 
def transform_scd1(df):
    """
    SCD Type 1: Overwrite existing values with new values.
    No history is preserved.
    """
    logging.info("Transforming data using SCD Type 1 (overwrite)")
    return df.copy()
 
def transform_scd2(df, existing_df):
    """
    SCD Type 2: Track historical changes.
    Adds new rows with new surrogate keys and date ranges.
    """
    logging.info("Transforming data using SCD Type 2 (historical tracking)")
    now = datetime.now()
    result = []
 
    df.columns = df.columns.astype(str).str.lower()
    existing_df.columns = existing_df.columns.astype(str).str.lower()
 
    print("df columns:", df.columns.tolist())
    print("existing_df columns:", existing_df.columns.tolist())
 
    if existing_df.empty or len(existing_df.columns) == 0:
        for _, row in df.iterrows():
            new_row = row.copy()
            new_row['row_id'] = str(uuid.uuid4())
            new_row['start_date'] = now
            new_row['end_date'] = None
            new_row['is_current'] = 1
            result.append(new_row)
        return pd.DataFrame(result)
 
    cust_id_col = 'customer_id' if 'customer_id' in existing_df.columns else 'customer_id'
 
    for _, row in df.iterrows():
        match = existing_df[existing_df[cust_id_col] == row['customer_id']]
 
        if not match.empty:
            existing_row = match.iloc[0]
            if existing_row['email'] != row['email'] or existing_row['address'] != row['address']:
                old_version = existing_row.copy()
                old_version['is_current'] = 0
                old_version['end_date'] = now
                result.append(old_version)
 
                new_version = row.copy()
                new_version['row_id'] = str(uuid.uuid4())
                new_version['start_date'] = now
                new_version['end_date'] = None
                new_version['is_current'] = 1
                result.append(new_version)
        else:
            new_row = row.copy()
            new_row['id'] = str(uuid.uuid4())
            new_row['start_date'] = now
            new_row['end_date'] = None
            new_row['is_current'] = 1
            result.append(new_row)
 
    return pd.DataFrame(result)
 
def transform_scd3(df, existing_df):
    """
    SCD Type 3: Track previous value in the same row.
    """
    logging.info("Transforming data using SCD Type 3 (previous value tracking)")
    result = []
 
    df.columns = df.columns.astype(str).str.lower()
    existing_df.columns = existing_df.columns.astype(str).str.lower()
 
    print("df columns:", df.columns.tolist())
    print(" existing_df columns:", existing_df.columns.tolist())
 
    if existing_df.empty or len(existing_df.columns) == 0:
        for _, row in df.iterrows():
            row['previous_email'] = None
            result.append(row)
        return pd.DataFrame(result)
 
    cust_id_col = 'customer_id' if 'customer_id' in existing_df.columns else 'customer_id'
 
    for _, row in df.iterrows():
        match = existing_df[existing_df[cust_id_col] == row['customer_id']]
        if not match.empty:
            existing_row = match.iloc[0]
            if existing_row['email'] != row['email']:
                updated_row = row.copy()
                updated_row['previous_email'] = existing_row['email']
                result.append(updated_row)
        else:
            row['previous_email'] = None
            result.append(row)
 
    return pd.DataFrame(result)