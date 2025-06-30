from extract import extract_documents
import pandas as pd
def transform_documents():
    df=extract_documents()
    #duration days
    df['no_of_days']=(pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.days
    #no of technologies
    df['tech_count'] = df['technologies'].apply(len)
    #duration categories
    df['duration_category'] = pd.cut(df['no_of_days'],bins=[0, 180, 300, float('inf')],labels=['Short', 'Medium', 'Long'])
    #mapping status
    status_map = {'Planned': 0, 'In Progress': 0.5, 'Completed': 1.0}
    df['completion_score'] = df['status'].map(status_map)
    #technologies arrays to strings
    df["technologies"] = df["technologies"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    print("transformation completed")
    return df
