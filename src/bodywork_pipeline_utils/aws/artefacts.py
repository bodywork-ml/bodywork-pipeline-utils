"""
Generic functions and models for interacting with AWS S3.
"""
import pickle
from datetime import datetime
from pathlib import Path
from pickle import PicklingError
from re import findall, sub
from typing import Any, Optional

import boto3 as aws
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

FILE_FORMAT_EXTENSIONS = {"csv": "CSV", "parquet": "PARQUET", "pkl": "PICKLE"}

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


def put_file_to_s3(
    path_to_file: str,
    bucket: str,
    folder: str = "",
    filename_override: Optional[str] = None,
) -> None:
    """Upload a file to S3.

    Args:
        path_to_file: Path to the file.
        bucket: Location on S3 to persist the object.
        folder: Folder within the bucket, defaults to "".
        filename_override: Optional override for the file's name on S3,
            defaults to None.

    Raises:
        FileExistsError: If the file could not be found.
        RuntimeError: If the object could not be uploaded to AWS S3.
    """
    path = Path(path_to_file)
    s3_filename = path.name if not filename_override else filename_override
    if not path.exists():
        msg = f"Cannot open file at {path_to_file}."
        raise FileExistsError(msg)
    folder_std = folder if folder.endswith("/") or folder == "" else f"{folder}/"
    s3_key = folder_std + s3_filename
    try:
        s3_client.upload_file(path_to_file, Bucket=bucket, Key=s3_key)
    except ClientError as e:
        msg = f"could upload file to AWS S3 - {e}"
        raise RuntimeError(msg)


def make_timestamped_filename(prefix: str, ref_date: datetime, file_format: str) -> str:
    return f"{prefix}_{ref_date.isoformat(sep='T', timespec='seconds')}.{file_format}"


class S3TimestampedArtefact:
    """Model for remote artefacts on S3."""

    def __init__(self, bucket: str, s3_obj_key: str, s3_etag: str):
        """Constructor.

        Args:
            bucket: S3 bucket name.
            s3_obj_key: Key of object in bucket.
            s3_etag: S3 entity tag (hash of the object).

        Raises:
            ValueError: If timestamp and a supported file format cannot
                be parsed from the object key.
        """
        self._bucket = bucket
        self._s3_obj_key = s3_obj_key
        self._s3_etag = s3_etag
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

    @property
    def etag(self) -> str:
        return self._s3_etag

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


def find_latest_artefact_on_s3(
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
                artefact = S3TimestampedArtefact(bucket, s3_obj["Key"], s3_obj["ETag"])
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
