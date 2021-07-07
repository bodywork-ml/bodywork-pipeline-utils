"""
Utility functions for working with AWS services.
"""
from datetime import datetime
from typing import Optional
from re import findall, sub

import boto3 as aws
from pandas import DataFrame


def get_latest_dataset_from_s3(bucket: str, folder: str) -> DataFrame:
    pass


def _extract_iso_timestamp_from_string(s: str) -> Optional[datetime]:
    ts_string = findall(r"\d{4}\W\d{2}\W\d{2}[\sT]\d{2}\W\d{2}\W\d{2}", s)
    if ts_string:
        ts_std = sub(r"[\sT](\d{2})\W(\d{2})\W(\d{2})", r"T\1:\2:\3", ts_string[0])
        return datetime.fromisoformat(ts_std)
    else:
        return None
