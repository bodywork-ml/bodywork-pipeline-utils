"""
Utility functions for working with AWS services.
"""
import pickle
from pickle import dumps, PicklingError
from datetime import datetime
from typing import Any, NamedTuple, Optional
from re import findall, sub

import boto3 as aws
from botocore.exceptions import ClientError
from pandas import DataFrame, read_csv, read_parquet


FILE_FORMAT_EXTENSIONS = {"csv": "CSV", "parquet": "PARQUET"}

s3_client = aws.client("s3")


class Dataset(NamedTuple):
    """Container for downloaded datasets."""

    data: DataFrame
    datetime: datetime
    key: str


class S3DatasetObject:
    """Model for remote tabular datasets on S3."""

    def __init__(self, bucket: str, s3_obj_key: str):
        """Constructor.

        Args:
            bucket: S3 bucket name.
            s3_obj_key: Key of object in bucket.

        Raises:
            ValueError: If timestamp and a supported file format cannot
                be parsed from the object key.
        """
        self._bucket = bucket
        self._s3_obj_key = s3_obj_key
        _datetime = self._extract_iso_timestamp_from_string(s3_obj_key)
        if _datetime is None:
            msg = f"{s3_obj_key} has no parsable timestamp."
            raise ValueError(msg)
        _file_format = self._extract_file_format_from_string(s3_obj_key)
        if _file_format is None:
            msg = f"{s3_obj_key} has no supported file format."
            raise ValueError(msg)
        self._datetime = _datetime
        self._file_format = _file_format

    def get(self) -> DataFrame:
        """Get dataset from S3.

        Raises:
            RuntimeError: If the dataset cannot be retrieved from S3.

        Returns:
            DataFrame representation of remote dataset.
        """
        try:
            s3_object = s3_client.get_object(Bucket=self._bucket, Key=self._s3_obj_key)
            data_stream = s3_object["Body"]
        except ClientError as e:
            msg = f"cannot get data from s3://{self._bucket}/{self._s3_obj_key} - {e}"
            raise RuntimeError(msg)
        if self._file_format == "CSV":
            dataset = read_csv(data_stream)
        elif self._file_format == "PARQUET":
            dataset = read_parquet(data_stream)
        return dataset

    @property
    def timestamp(self) -> datetime:
        return self._datetime

    @property
    def file_format(self) -> str:
        return self._file_format

    @property
    def obj_key(self) -> str:
        return self._s3_obj_key

    def __lt__(self, other: "S3DatasetObject") -> bool:
        return self._datetime < other._datetime

    @staticmethod
    def _extract_iso_timestamp_from_string(s: str) -> Optional[datetime]:
        regex = r"\d{4}\W\d{2}\W\d{2}[\sT]\d{2}\W\d{2}\W\d{2}|\d{4}\W\d{2}\W\d{2}"
        ts_string = findall(regex, s)
        if ts_string:
            ts_std = sub(r"[\sT](\d{2})\W(\d{2})\W(\d{2})", r"T\1:\2:\3", ts_string[0])
            return datetime.fromisoformat(ts_std)
        else:
            return None

    @staticmethod
    def _extract_file_format_from_string(s: str) -> Optional[str]:
        file_format_extension = findall(r"\.(\w+)$", s)
        if file_format_extension:
            return FILE_FORMAT_EXTENSIONS.get(file_format_extension[0].lower())
        else:
            return None


def get_latest_dataset_from_s3(bucket: str, folder: str = "") -> Dataset:
    """Get the most recent timestamped dataset from an S3 folder.

    Args:
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Raises:
        RuntimeError: If no files are found or if there was an error
            connecting to AWS.

    Returns:
        A Dataset object containing the data and metadata about the file.
    """
    folder_std = folder if folder.endswith("/") or folder == "" else f"{folder}/"
    try:
        s3_objects = s3_client.list_objects(Bucket=bucket, Prefix=folder_std)
        s3_datasets = []
        for s3_obj in s3_objects["Contents"]:
            try:
                dataset_obj = S3DatasetObject(bucket, s3_obj["Key"])
                s3_datasets.append(dataset_obj)
            except ValueError:
                pass

        latest_file = sorted(s3_datasets)[-1]
        datset = Dataset(latest_file.get(), latest_file.timestamp, latest_file.obj_key)
        return datset
    except IndexError:
        msg = f"no valid dataset files found in s3://{bucket}/{folder_std}"
        raise RuntimeError(msg)
    except Exception as e:
        msg = f"failed to download dataset from s3://{bucket}/{folder_std}"
        raise RuntimeError(msg) from e


def put_object_to_s3(obj: Any, file_name: str, bucket: str, folder: str = "") -> None:
    folder_std = folder if folder.endswith("/") or folder == "" else f"{folder}/"
    s3_key = folder_std + file_name
    try:
        obj_bytes = pickle.dumps(obj, protocol=5)
        s3_client.put_object(Body=obj_bytes, Bucket=bucket, Key=s3_key)
    except PicklingError as e:
        msg = f"could not serialise object to bytes with pickle - {e}"
        raise RuntimeError(msg)
    except ClientError as e:
        msg = f"could upload object to AWS S3 - {e}"
        raise RuntimeError(msg)
