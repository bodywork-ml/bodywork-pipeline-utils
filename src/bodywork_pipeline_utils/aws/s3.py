"""
Generic functions and models for interacting with AWS S3.
"""
import pickle
from pathlib import Path
from pickle import PicklingError
from datetime import datetime
from typing import Any

import boto3 as aws
from botocore.exceptions import ClientError


s3_client = aws.client("s3")


def put_object_to_s3(obj: Any, file_name: str, bucket: str, folder: str = "") -> None:
    """Pickle and persist an object to S3.

    Args:
        obj: The object to pickle and persist to S3.
        file_name: The name to give to the pickled file.
        bucket: Location on S3 to persist the object.
        folder: Folder within the bucket, defaults to "".

    Raises:
        RuntimeError: If the object could not be pickled or AWS S3 could
             not be accessed.
    """
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


def put_file_to_s3(path_to_file: str, bucket: str, folder: str = "") -> None:
    """Upload a file to S3.

    Args:
        path_to_file: Path to the file.
        bucket: Location on S3 to persist the object.
        folder: Folder within the bucket, defaults to "".

    Raises:
        FileExistsError: If the file could not be found.
        RuntimeError: If the object could not be uploaded to AWS S3.
    """
    path = Path(path_to_file)
    if not path.exists():
        msg = f"Cannot open file at {path_to_file}."
        raise FileExistsError(msg)
    folder_std = folder if folder.endswith("/") or folder == "" else f"{folder}/"
    s3_key = folder_std + path.name
    try:
        s3_client.upload_file(path_to_file, Bucket=bucket, Key=s3_key)
    except ClientError as e:
        msg = f"could upload file to AWS S3 - {e}"
        raise RuntimeError(msg)


def make_timestamped_filename(
    prefix: str, ref_date: datetime, file_format: str
) -> str:
    return f"{prefix}_{ref_date.isoformat(sep='T', timespec='seconds')}.{file_format}"
