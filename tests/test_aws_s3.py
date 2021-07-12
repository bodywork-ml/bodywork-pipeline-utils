"""
Tests for generic AWS S3 interaction.
"""
import pickle
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from pytest import raises

from bodywork_pipeline_utils.aws.s3 import (
    make_timestamped_filename,
    put_object_to_s3,
    put_file_to_s3
)


@patch("bodywork_pipeline_utils.aws.s3.s3_client")
def test_put_object_to_s3_puts_object(mock_s3_client: MagicMock):
    some_object = [1, 2, 3, 4, 5]
    put_object_to_s3(some_object, "my_object.pickle", "my-bucket", "stuff/")
    mock_s3_client.put_object.assert_called_once_with(
        Body=pickle.dumps(some_object, protocol=5),
        Bucket="my-bucket",
        Key="stuff/my_object.pickle",
    )


@patch("bodywork_pipeline_utils.aws.s3.s3_client")
def test_put_object_to_s3_raises_exception_when_upload_fails(mock_s3_client: MagicMock):
    mock_s3_client.put_object.side_effect = ClientError({}, "")
    with raises(RuntimeError, match="could upload object to AWS S3"):
        put_object_to_s3([1, 2, 3, 4, 5], "my_object.pickle", "my-bucket", "stuff/")


@patch("bodywork_pipeline_utils.aws.s3.pickle")
def test_put_object_to_s3_raises_exception_when_pickle_fails(mock_pickler: MagicMock):
    mock_pickler.dumps.side_effect = pickle.PicklingError()
    with raises(RuntimeError, match="could not serialise object to bytes with pickle"):
        put_object_to_s3([1, 2, 3], "my_object.pickle", "my-bucket", "stuff/")


@patch("bodywork_pipeline_utils.aws.s3.s3_client")
def test_put_file_to_s3_uploads_file(mock_s3_client: MagicMock):
    put_file_to_s3("tests/resources/dataset.csv", "my-bucket", "stuff/")
    mock_s3_client.upload_file.assert_called_once_with(
        "tests/resources/dataset.csv",
        Bucket="my-bucket",
        Key="stuff/dataset.csv",
    )


@patch("bodywork_pipeline_utils.aws.s3.s3_client")
def test_put_file_to_s3_uploads_file_with_file_name_override(mock_s3_client: MagicMock):
    put_file_to_s3("tests/resources/dataset.csv", "my-bucket", "stuff/", "foo.bar")
    mock_s3_client.upload_file.assert_called_once_with(
        "tests/resources/dataset.csv",
        Bucket="my-bucket",
        Key="stuff/foo.bar",
    )


@patch("bodywork_pipeline_utils.aws.s3.s3_client")
def test_put_file_to_s3_raises_exception_when_upload_fails(mock_s3_client: MagicMock):
    mock_s3_client.upload_file.side_effect = ClientError({}, "")
    with raises(RuntimeError, match="could upload file to AWS S3"):
        put_file_to_s3("tests/resources/dataset.csv", "my-bucket", "stuff/")
