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
from botocore.response import StreamingBody
from pandas import DataFrame, read_csv, read_parquet


FILE_FORMAT_EXTENSIONS = {"csv": "CSV", "parquet": "PARQUET", "pkl": "PICKLE"}

s3_client = aws.client("s3")


class S3TimestampedArtefact:
    """Model for remote artefacts on S3."""

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

    def get(self) -> StreamingBody:
        """Get artefact from S3.

        Raises:
            RuntimeError: If the artefact cannot be retrieved from S3.

        Returns:
            Streamed bytes representation of remote artefact.
        """
        try:
            s3_object = s3_client.get_object(Bucket=self._bucket, Key=self._s3_obj_key)
            return s3_object["Body"]
        except ClientError as e:
            msg = (
                f"cannot get artefact from s3://{self._bucket}/{self._s3_obj_key} - {e}"
            )
            raise RuntimeError(msg)

    @property
    def timestamp(self) -> datetime:
        return self._datetime

    @property
    def file_format(self) -> str:
        return self._file_format

    @property
    def obj_key(self) -> str:
        return self._s3_obj_key

    def __lt__(self, other: "S3TimestampedArtefact") -> bool:
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


def _find_latest_artefact_on_s3(
    file_format: str, bucket: str, folder: str = ""
) -> S3TimestampedArtefact:
    """Get the most recent timestamped artefact from an S3 folder.

    Args:
        file_format: File format associated with artefact.
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Raises:
        RuntimeError: If no files are found or if there was an error
            connecting to AWS.
        ValueError: If file_format is not a supported file format.

    Returns:
        An artefact object.
    """
    if file_format not in FILE_FORMAT_EXTENSIONS.keys():
        msg = f"{file_format} is not a supported file type."
        raise ValueError(msg)
    else:
        file_format_ = FILE_FORMAT_EXTENSIONS[file_format]
    folder_std = folder if folder.endswith("/") or folder == "" else f"{folder}/"
    try:
        s3_objects = s3_client.list_objects(Bucket=bucket, Prefix=folder_std)
        s3_artefacts = []
        for s3_obj in s3_objects["Contents"]:
            try:
                artefact = S3TimestampedArtefact(bucket, s3_obj["Key"])
                if artefact.file_format == file_format_:
                    s3_artefacts.append(artefact)
            except ValueError:
                pass

        latest_artefact = sorted(s3_artefacts)[-1]
        return latest_artefact
    except IndexError:
        msg = f"no valid artefacts found in s3://{bucket}/{folder_std}"
        raise RuntimeError(msg)
    except Exception as e:
        msg = f"failed to download dataset from s3://{bucket}/{folder_std}"
        raise RuntimeError(msg) from e


class Dataset(NamedTuple):
    """Container for downloaded datasets and associated metadata."""

    data: DataFrame
    datetime: datetime
    key: str


def get_latest_csv_dataset_from_s3(bucket: str, folder: str = "") -> Dataset:
    """Get the latest CSV dataset from S3.

    Args:
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Returns:
        Dataset object.
    """
    artefact = _find_latest_artefact_on_s3("csv", bucket, folder)
    data = read_csv(artefact.get())
    return Dataset(data, artefact.timestamp, artefact.obj_key)


def get_latest_parquet_dataset_from_s3(bucket: str, folder: str = "") -> Dataset:
    """Get the latest Parquet dataset from S3.

    Args:
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Returns:
        Dataset object.
    """
    artefact = _find_latest_artefact_on_s3("parquet", bucket, folder)
    data = read_parquet(artefact.get())
    return Dataset(data, artefact.timestamp, artefact.obj_key)


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
