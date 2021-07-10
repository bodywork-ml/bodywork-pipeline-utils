"""
Tests for AWS utilities.
"""
import pickle
from datetime import datetime
from typing import Protocol
from unittest import mock
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from pandas import DataFrame, read_csv

from pytest import raises

from bodywork_pipeline_utils.aws.datasets import (
    _find_latest_artefact_on_s3,
    get_latest_csv_dataset_from_s3,
    get_latest_parquet_dataset_from_s3,
    put_object_to_s3,
    S3TimestampedArtefact,
    put_file_to_s3,
)


def test_extract_iso_timestamp_from_string_identifies_iso_timestamps():
    filename1 = "2021-07-07T13:33:13.csv"
    filename2 = "some_dataset_2021-07-07T13:33:13.csv"
    filename3 = "some_dataset_2021-07-07 13|33|13.csv"
    filename4 = "there_is_no_date_here.csv"
    expected_dt = datetime(2021, 7, 7, 13, 33, 13)
    assert (
        S3TimestampedArtefact._extract_iso_timestamp_from_string(filename1)
        == expected_dt
    )
    assert (
        S3TimestampedArtefact._extract_iso_timestamp_from_string(filename2)
        == expected_dt
    )
    assert (
        S3TimestampedArtefact._extract_iso_timestamp_from_string(filename3)
        == expected_dt
    )
    assert S3TimestampedArtefact._extract_iso_timestamp_from_string(filename4) is None


def test_extract_iso_timestamp_from_string_identifies_iso_dates():
    filename1 = "2021-07-07.csv"
    filename2 = "some_dataset_2021-07-07.csv"
    expected_dt = datetime(2021, 7, 7)
    assert (
        S3TimestampedArtefact._extract_iso_timestamp_from_string(filename1)
        == expected_dt
    )
    assert (
        S3TimestampedArtefact._extract_iso_timestamp_from_string(filename2)
        == expected_dt
    )


def test_extract_file_format_from_string_matches_file_formats():
    filename1 = "2021-07-07.csv"
    filename2 = "2021-07-07.parquet"
    filename3 = "unknown_file_format"
    filename4 = "unsupported_file.format"
    assert S3TimestampedArtefact._extract_file_format_from_string(filename1) == "CSV"
    assert (
        S3TimestampedArtefact._extract_file_format_from_string(filename2) == "PARQUET"
    )
    assert S3TimestampedArtefact._extract_file_format_from_string(filename3) is None
    assert S3TimestampedArtefact._extract_file_format_from_string(filename4) is None


def test_S3TimestampedArtefact_raises_exception_for_invalid_type_keys():
    with raises(ValueError, match="has no parsable timestamp"):
        S3TimestampedArtefact("my-bucket", "this_is_not_a_dataset_key", "hash")

    with raises(ValueError, match="has no supported file format"):
        S3TimestampedArtefact("my-bucket", "my/data_2021-12-12T13:30:30", "hash")

    with raises(ValueError, match="has no supported file format"):
        S3TimestampedArtefact("my-bucket", "my/data_2021-12-12T13:30:30.foo", "hash")


def test_S3TimestampedArtefact_construction_with_valid_data():
    assert (
        S3TimestampedArtefact("my-bucket", "my_data_2021-12-12T13:30:30.csv", "hash")
        is not None
    )


def test_S3TimestampedArtefact_less_than_operator():
    obj1 = S3TimestampedArtefact("my-bucket", "my_data_2021-11-12T13:30:30.csv", "hash")
    obj2 = S3TimestampedArtefact("my-bucket", "my_data_2021-12-12T13:30:30.csv", "hash")
    assert obj1 < obj2


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_S3TimestampedArtefact_gets_artefacts(mock_s3_client: MagicMock):
    mock_s3_client.get_object.return_value = {
        "Body": open("tests/resources/dataset.csv", "rb")
    }
    artefact = S3TimestampedArtefact(
        "my-bucket", "my/data_2020-01-01T00:00:00.csv", "hash"
    )
    data = artefact.get()
    df = read_csv(data)
    assert type(df) == DataFrame
    assert df.shape == (2, 2)


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_S3TimestampedArtefact_raises_exception_on_s3_client_error(
    mock_s3_client: MagicMock,
):
    mock_s3_client.get_object.side_effect = ClientError({}, "")
    artefact = S3TimestampedArtefact(
        "my-bucket", "my/data_2020-01-01T00:00:00.csv", "hash"
    )
    with raises(RuntimeError, match="cannot get artefact from s3"):
        artefact.get()


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_find_latest_artefact_on_s3_finds_latest_artefact(mock_s3_client: MagicMock):
    mock_s3_client.list_objects.return_value = {
        "Contents": [
            {"Key": "datasets/my_data_2020-06-08T00:00:00.csv", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-07-08T01:00:00.csv", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-07-08T01:01:00.parquet", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-05-08T00:15:00.csv", "ETag": "hash"},
        ]
    }
    artefact = _find_latest_artefact_on_s3("csv", "my-bucket", "datasets")
    assert artefact.file_format == "CSV"
    assert artefact.timestamp == datetime(2020, 7, 8, 1)
    assert artefact.obj_key == "datasets/my_data_2020-07-08T01:00:00.csv"


def test_find_latest_artefact_on_s3_raises_exception_for_invalid_file_format():
    with raises(ValueError, match="is not a supported file type"):
        _find_latest_artefact_on_s3("foobar", "my-bucket", "datasets")


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_get_latest_csv_dataset_from_s3_return_dataset(mock_s3_client: MagicMock):
    mock_s3_client.list_objects.return_value = {
        "Contents": [
            {"Key": "datasets/my_data_2020-06-08T00:00:00.csv", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-07-08T01:00:00.csv", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-05-08T00:15:00.csv", "ETag": "hash"},
        ]
    }
    mock_s3_client.get_object.return_value = {
        "Body": open("tests/resources/dataset.csv", "rb")
    }
    dataset = get_latest_csv_dataset_from_s3("my-bucket", "my-folder")
    assert type(dataset.data) == DataFrame
    assert dataset.data.shape == (2, 2)
    assert dataset.datetime == datetime(2020, 7, 8, 1)
    assert dataset.key == "datasets/my_data_2020-07-08T01:00:00.csv"
    assert dataset.hash == "hash"


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_get_latest_parquet_dataset_from_s3_return_dataset(mock_s3_client: MagicMock):
    mock_s3_client.list_objects.return_value = {
        "Contents": [
            {"Key": "datasets/my_data_2020-06-08T00:00:00.parquet", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-07-08T01:00:00.parquet", "ETag": "hash"},
            {"Key": "datasets/my_data_2020-05-08T00:15:00.parquet", "ETag": "hash"},
        ]
    }
    mock_s3_client.get_object.return_value = {
        "Body": open("tests/resources/dataset.parquet", "rb")
    }
    dataset = get_latest_parquet_dataset_from_s3("my-bucket", "my-folder")
    assert type(dataset.data) == DataFrame
    assert dataset.data.shape == (2, 2)
    assert dataset.datetime == datetime(2020, 7, 8, 1)
    assert dataset.key == "datasets/my_data_2020-07-08T01:00:00.parquet"
    assert dataset.hash == "hash"


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_find_latest_artefact_on_s3_raises_exception_when_no_datasets_found(
    mock_s3_client: MagicMock,
):
    mock_s3_client.list_objects.return_value = {"Contents": []}
    with raises(RuntimeError, match="no valid artefacts found in s3"):
        _find_latest_artefact_on_s3("csv", "my-bucket", "datasets")


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_find_latest_artefact_on_s3_raises_exception_when_s3_client_fails(
    mock_s3_client: MagicMock,
):
    mock_s3_client.list_objects.side_effect = Exception
    with raises(RuntimeError, match="failed to download dataset"):
        _find_latest_artefact_on_s3("csv", "my-bucket", "datasets")


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_put_object_to_s3_puts_object(mock_s3_client: MagicMock):
    some_object = [1, 2, 3, 4, 5]
    put_object_to_s3(some_object, "my_object.pickle", "my-bucket", "stuff/")
    mock_s3_client.put_object.assert_called_once_with(
        Body=pickle.dumps(some_object, protocol=5),
        Bucket="my-bucket",
        Key="stuff/my_object.pickle",
    )


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_put_object_to_s3_raises_exception_when_upload_fails(mock_s3_client: MagicMock):
    mock_s3_client.put_object.side_effect = ClientError({}, "")
    with raises(RuntimeError, match="could upload object to AWS S3"):
        put_object_to_s3([1, 2, 3, 4, 5], "my_object.pickle", "my-bucket", "stuff/")


@patch("bodywork_pipeline_utils.aws.datasets.pickle")
def test_put_object_to_s3_raises_exception_when_pickle_fails(mock_pickler: MagicMock):
    mock_pickler.dumps.side_effect = pickle.PicklingError()
    with raises(RuntimeError, match="could not serialise object to bytes with pickle"):
        put_object_to_s3([1, 2, 3], "my_object.pickle", "my-bucket", "stuff/")


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_put_file_to_s3_uploads_file(mock_s3_client: MagicMock):
    put_file_to_s3("tests/resources/dataset.csv", "my-bucket", "stuff/")
    mock_s3_client.upload_file.assert_called_once_with(
        "tests/resources/dataset.csv",
        Bucket="my-bucket",
        Key="stuff/dataset.csv",
    )


@patch("bodywork_pipeline_utils.aws.datasets.s3_client")
def test_put_file_to_s3_raises_exception_when_upload_fails(mock_s3_client: MagicMock):
    mock_s3_client.upload_file.side_effect = ClientError({}, "")
    with raises(RuntimeError, match="could upload file to AWS S3"):
        put_file_to_s3("tests/resources/dataset.csv", "my-bucket", "stuff/")


def test_put_file_to_s3_raises_exception_when_file_cannot_be_founbd():
    with raises(FileExistsError, match="Cannot open file"):
        put_file_to_s3("tests/resources/foo.csv", "my-bucket", "stuff/")
