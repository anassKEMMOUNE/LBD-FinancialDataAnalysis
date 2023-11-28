import pdcast as pdc
import pandas as pd

def df_optimize(df_path) :
    df = pd.read_csv(df_path)
    df = pdc.downcast(df)
    return df

def is_unique_key(df,columnName) :
    x = df.shape[0]
    l = df[columnName].drop_duplicates().shape[0]
    return x == l  