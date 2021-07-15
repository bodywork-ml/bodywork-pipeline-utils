"""
Tests for AWS model management.
"""
from datetime import datetime
import pickle
from hashlib import md5
from unittest.mock import ANY, MagicMock, patch

from pandas import DataFrame
from pytest import fixture, raises
from sklearn.tree import DecisionTreeRegressor

from bodywork_pipeline_utils.aws.models import get_latest_pkl_model_from_s3, Model
from bodywork_pipeline_utils.aws.datasets import Dataset


@fixture(scope="function")
def dataset() -> Dataset:
    dataset = Dataset(
        DataFrame({"y": [1, 2, 3], "x": ["a", "b", "c"]}),
        datetime(2021, 1, 1),
        "my-bucket",
        "datasets/the-data-2021-01-01T00:00:00.csv",
        "the-aws-s3-hash-value",
    )
    return dataset


def test_compute_model_hash_computes_hashes():
    obj = "hello, world"
    obj_bytes = pickle.dumps(obj, protocol=5)
    hash_algo = md5(obj_bytes)
    assert Model._compute_model_hash(obj) == hash_algo.hexdigest()


@patch("bodywork_pipeline_utils.aws.models.dumps")
def test_compute_model_hash_raises_exception_if_pickling_fails(mock_patch: MagicMock):
    mock_patch.side_effect = pickle.PicklingError()
    with raises(RuntimeError, match="Could not pickle"):
        Model._compute_model_hash("foo")


@patch("bodywork_pipeline_utils.aws.models.md5")
def test_compute_model_hash_raises_exception_if_hashing_fails(mock_md5: MagicMock):
    mock_md5.side_effect = Exception()
    with raises(RuntimeError, match="Could not hash"):
        Model._compute_model_hash("foo")


def test_models_can_be_created(dataset: Dataset):
    try:
        Model("my_model", DecisionTreeRegressor(), dataset, {"foo": "bar"})
        assert True
    except Exception:
        assert False


def test_model_equality_operator(dataset: Dataset):
    new_dataset = Dataset(
        DataFrame({"y": [1, 2, 3], "x": ["a", "b", "c"]}),
        datetime(2021, 1, 1),
        "my-bucket",
        "datasets/the-data-2021-01-01T00:00:00.csv",
        "new-aws-s3-hash-value",
    )

    model1 = Model("model1", DecisionTreeRegressor(), dataset)
    model2 = Model("model2", DecisionTreeRegressor(), dataset)
    model3 = Model("model1", DecisionTreeRegressor(random_state=42), dataset)
    model4 = Model("model1", DecisionTreeRegressor(), new_dataset)
    assert model1 == model1
    assert model1 != model2
    assert model1 != model3
    assert model1 != model4


@patch("bodywork_pipeline_utils.aws.models.md5")
@patch("bodywork_pipeline_utils.aws.models.datetime")
def test_model_str_representation(
    mock_datetime: MagicMock, mock_md5: MagicMock, dataset: Dataset
):
    mock_md5().hexdigest.return_value = "foobar"
    mock_datetime.now.return_value = datetime(2021, 1, 1)
    model = Model("the-model", DecisionTreeRegressor(), dataset)
    model_str = str(model)
    assert "the-model" in model_str
    assert "DecisionTreeRegressor" in model_str
    assert "2021-01-01T00:00:00" in model_str
    assert "the-aws-s3-hash-value" in model_str
    assert "git" in model_str
    assert "datasets/the-data-2021-01-01T00:00:00.csv" in model_str
    assert "foobar" in model_str


@patch("bodywork_pipeline_utils.aws.models.put_file_to_s3")
@patch("bodywork_pipeline_utils.aws.models.datetime")
def test_put_model_to_s3_puts_object_to_s3(
    mock_datetime: MagicMock, mock_func: MagicMock, dataset: Dataset
):
    mock_datetime.now.return_value = datetime(2021, 1, 1)
    model = Model("model", DecisionTreeRegressor(), dataset)
    s3_location = model.put_model_to_s3("my-bucket", "models")
    mock_func.assert_called_once_with(
        ANY, "my-bucket", "models", "model_2021-01-01T00:00:00.pkl"
    )
    assert s3_location == "my-bucket/models/model_2021-01-01T00:00:00.pkl"


@patch("bodywork_pipeline_utils.aws.artefacts.s3_client")
def test_get_latest_model_from_s3_gets_latest_model(mock_s3_client: MagicMock):
    mock_s3_client.list_objects.return_value = {
        "Contents": [
            {"Key": "models/my_model_2020-06-08T00:00:00.pkl", "ETag": "hash"},
            {"Key": "model/my_model_2020-07-08T01:00:00.pkl", "ETag": "hash"},
            {"Key": "model/my_model_2020-05-08T00:15:00.pkl", "ETag": "hash"},
        ]
    }
    mock_s3_client.get_object.return_value = {
        "Body": open("tests/resources/model.pkl", "rb")
    }
    model = get_latest_pkl_model_from_s3("my-bucket", "my-folder")
    assert isinstance(model, Model)


@patch("bodywork_pipeline_utils.aws.artefacts.s3_client")
def test_get_latest_model_from_s3_raises_exception_if_artefact_not_model_type(
    mock_s3_client: MagicMock,
):
    mock_s3_client.list_objects.return_value = {
        "Contents": [
            {"Key": "models/my_model_2020-06-08T00:00:00.pkl", "ETag": "hash"},
            {"Key": "model/my_model_2020-07-08T01:00:00.pkl", "ETag": "hash"},
            {"Key": "model/my_model_2020-05-08T00:15:00.pkl", "ETag": "hash"},
        ]
    }
    mock_s3_client.get_object.return_value = {
        "Body": open("tests/resources/not_model.pkl", "rb")
    }
    with raises(RuntimeError, match="not type Model"):
        get_latest_pkl_model_from_s3("my-bucket", "my-folder")


@patch("bodywork_pipeline_utils.aws.artefacts.s3_client")
@patch("bodywork_pipeline_utils.aws.models.loads")
def test_get_latest_model_from_s3_raises_exception_if_artefact_is_corrupted(
    mock_pickle_loads: MagicMock, mock_s3_client: MagicMock
):
    mock_s3_client.list_objects.return_value = {
        "Contents": [
            {"Key": "models/my_model_2020-06-08T00:00:00.pkl", "ETag": "hash"},
            {"Key": "model/my_model_2020-07-08T01:00:00.pkl", "ETag": "hash"},
            {"Key": "model/my_model_2020-05-08T00:15:00.pkl", "ETag": "hash"},
        ]
    }
    mock_s3_client.get_object.return_value = {
        "Body": open("tests/resources/model.pkl", "rb")
    }
    mock_pickle_loads.side_effect = pickle.UnpicklingError()
    with raises(RuntimeError, match="could not be unpickled"):
        get_latest_pkl_model_from_s3("my-bucket", "my-folder")
