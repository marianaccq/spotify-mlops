"""
Creator: Mariana
Date: Fev. 2022
Download the raw data
"""
import pandas as pd
import scipy.stats


def test_column_presence_and_type(data):
    print(type(data))
    # Disregard the reference dataset
    df = data

    required_columns = {
        "danceability": pd.api.types.is_float_dtype,
        "energy": pd.api.types.is_float_dtype,
        "loudness": pd.api.types.is_float_dtype,
        "speechiness": pd.api.types.is_float_dtype,
        "acousticness": pd.api.types.is_float_dtype,
        "instrumentalness": pd.api.types.is_float_dtype,
        "liveness": pd.api.types.is_float_dtype,
        "valence": pd.api.types.is_float_dtype,
        "duration_ms": pd.api.types.is_int64_dtype,
        "genre": pd.api.types.is_object_dtype,
      
    }

    # Check column presence
    assert set(df.columns.values).issuperset(set(required_columns.keys()))

    for col_name, format_verification_funct in required_columns.items():

        assert format_verification_funct(df[col_name]), f"Column {col_name} failed test {format_verification_funct}"

# Deterministic Test
def test_class_names(data):
    
    # Disregard the reference dataset
    df = data

    # Check that only the known classes are present
    known_classes = [
        'Rap',
        'Pop',
        'Hiphop',
        'trance',
        'trap'
    ]

    assert df["genre"].isin(known_classes).all()

    
    
# mlflow run . -P reference_artifact=spotify-mlops-preprocess/data_preprocessed:latest
