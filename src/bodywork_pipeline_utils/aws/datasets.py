"""
Utility functions for working with AWS services.
"""
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any, NamedTuple

from pandas import DataFrame, read_csv, read_parquet

from bodywork_pipeline_utils.aws.artefacts import (
    find_latest_artefact_on_s3,
    make_timestamped_filename,
    put_file_to_s3,
)


class Dataset(NamedTuple):
    """Container for downloaded datasets and associated metadata."""

    data: DataFrame
    datetime: datetime
    bucket: str
    key: str
    hash: str


def get_latest_csv_dataset_from_s3(bucket: str, folder: str = "") -> Dataset:
    """Get the latest CSV dataset from S3.

    Args:
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Returns:
        Dataset object.
    """
    artefact = find_latest_artefact_on_s3("csv", bucket, folder)
    data = read_csv(artefact.get())
    return Dataset(data, artefact.timestamp, bucket, artefact.obj_key, artefact.etag)


def get_latest_parquet_dataset_from_s3(bucket: str, folder: str = "") -> Dataset:
    """Get the latest Parquet dataset from S3.

    Args:
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Returns:
        Dataset object.
    """
    artefact = find_latest_artefact_on_s3("parquet", bucket, folder)
    data = read_parquet(artefact.get())
    return Dataset(data, artefact.timestamp, bucket, artefact.obj_key, artefact.etag)


def put_csv_dataset_to_s3(
    data: DataFrame,
    filename_prefix: str,
    ref_datetime: datetime,
    bucket: str,
    folder: str = "",
    **kwargs: Any,
) -> None:
    """Upload DataFrame to S3 as a CSV file.

    Args:
        data: The DataFrame to upload.
        filename_prefix: Prefix before datetime filename element.
        ref_datetime: The reference date associated with data.
        bucket: Location on S3 to persist the data.
        folder: Folder within the bucket, defaults to "".
        kwargs: Keywork arguments to pass to pandas.to_csv.
    """
    filename = make_timestamped_filename(filename_prefix, ref_datetime, "csv")
    with NamedTemporaryFile() as temp_file:
        data.to_csv(temp_file, **kwargs)
        put_file_to_s3(temp_file.name, bucket, folder, filename)


def put_parquet_dataset_to_s3(
    data: DataFrame,
    filename_prefix: str,
    ref_datetime: datetime,
    bucket: str,
    folder: str = "",
    **kwargs: Any,
) -> None:
    """Upload DataFrame to S3 as a Parquet file.

    Args:
        data: The DataFrame to upload.
        filename_prefix: Prefix before datetime filename element.
        ref_datetime: The reference date associated with data.
        bucket: Location on S3 to persist the data.
        folder: Folder within the bucket, defaults to "".
        kwargs: Keywork arguments to pass to pandas.to_csv.
    """
    filename = make_timestamped_filename(filename_prefix, ref_datetime, "parquet")
    with NamedTemporaryFile() as temp_file:
        data.to_parquet(temp_file, **kwargs)
        put_file_to_s3(temp_file.name, bucket, folder, filename)
