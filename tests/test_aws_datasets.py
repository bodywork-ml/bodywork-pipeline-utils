"""
Tests for AWS dataset management.
"""
from datetime import datetime
from unittest.mock import ANY, MagicMock, patch

from pandas import DataFrame

from pytest import raises

from bodywork_pipeline_utils.aws.datasets import (
    get_latest_csv_dataset_from_s3,
    get_latest_parquet_dataset_from_s3,
    put_csv_dataset_to_s3,
    put_parquet_dataset_to_s3,
)


@patch("bodywork_pipeline_utils.aws.artefacts.s3_client")
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
    assert dataset.bucket == "my-bucket"
    assert dataset.key == "datasets/my_data_2020-07-08T01:00:00.csv"
    assert dataset.hash == "hash"


@patch("bodywork_pipeline_utils.aws.artefacts.s3_client")
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
    assert dataset.bucket == "my-bucket"
    assert dataset.key == "datasets/my_data_2020-07-08T01:00:00.parquet"
    assert dataset.hash == "hash"


@patch("bodywork_pipeline_utils.aws.datasets.put_file_to_s3")
def test_put_csv_dataset_to_s3(mock_func: MagicMock):
    data = DataFrame({"x": [1, 2, 3], "c": ["a", "b", "c"]})
    data_date = datetime(2021, 7, 12, 13)
    put_csv_dataset_to_s3(
        data=data,
        filename_prefix="training_data",
        ref_datetime=data_date,
        bucket="my-bucket",
        folder="datasets",
    )
    mock_func.assert_called_once_with(
        ANY, "my-bucket", "datasets", "training_data_2021-07-12T13:00:00.csv"
    )


@patch("bodywork_pipeline_utils.aws.datasets.put_file_to_s3")
def test_put_parquet_dataset_to_s3(mock_func: MagicMock):
    data = DataFrame({"x": [1, 2, 3], "c": ["a", "b", "c"]})
    data_date = datetime(2021, 7, 12, 13)
    put_parquet_dataset_to_s3(
        data=data,
        filename_prefix="training_data",
        ref_datetime=data_date,
        bucket="my-bucket",
        folder="datasets",
    )
    mock_func.assert_called_once_with(
        ANY, "my-bucket", "datasets", "training_data_2021-07-12T13:00:00.parquet"
    )
