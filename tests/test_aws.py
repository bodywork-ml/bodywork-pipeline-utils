"""
Tests for AWS utilities.
"""
from datetime import datetime
from bodywork_pipeline_utils.aws import (
    _extract_iso_timestamp_from_string,
    get_latest_dataset_from_s3
)


def test_extract_iso_timestamp_from_string_identifies_iso_timestamps():
    filename1 = "2021-07-07T13:33:13.csv"
    filename2 = "some_dataset_2021-07-07T13:33:13.csv"
    filename3 = "some_dataset_2021-07-07 13|33|13.csv"
    filename4 = "there_is_no_date_here.csv"
    expected_dt = datetime(2021, 7, 7, 13, 33, 13)
    assert _extract_iso_timestamp_from_string(filename1) == expected_dt
    assert _extract_iso_timestamp_from_string(filename2) == expected_dt
    assert _extract_iso_timestamp_from_string(filename3) == expected_dt
    assert _extract_iso_timestamp_from_string(filename4) is None
