import pandas as pd


def document(df, *args, **kwargs):
    # Your function logic here
    print(f"Documenting DataFrame with shape {df.shape}")


def pandas_document():
    pd.DataFrame.document = document
