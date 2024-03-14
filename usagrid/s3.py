import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO
import pandas as pd

def write_data_to_s3_pyarrow(bucket_name:str, object_key:str,data:pd.DataFrame) -> None:

    """
    bucket_name: s3 bucket
    object_key: filepath
    data:dataframe
    """
    # Convert data to PyArrow Table
    table = pa.Table.from_pandas(data)  # Assuming data is in pandas DataFrame format

    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # # Write PyArrow Table to S3 in Parquet format
    # with pa.OSFile(object_key, 'wb') as f:
    #     pq.write_table(table, f)

    # Upload Parquet file to S3
    s3_client = boto3.client('s3')
    s3_client.upload_fileobj(Bucket=bucket_name,Key=object_key,Fileobj=buffer)


def list_files_in_folder(bucket_name, folder_name):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # List objects within the folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # Extract the file names from the response
    files = []
    for obj in response.get('Contents', []):
        files.append(obj['Key'])

    return files

def read_pyarrow_df_from_s3(bucket_name, key) -> pa.Table:
    """
    Reads a file from S3 and returns it as a PyArrow DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key (path) of the file in the S3 bucket.

    Returns:
        pyarrow.Table: A PyArrow DataFrame containing the data from the file.
    """
    # Download file from S3 to BytesIO
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    buffer = BytesIO(response['Body'].read())
    buffer.seek(0)

    # Load data into PyArrow DataFrame
    return pq.read_table(buffer)