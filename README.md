# Bodywork Pipeline Utilities

Utilities for helping with pipeline development and integration with 3rd party MLOps services.

```text
|-- aws
    |-- Dataset
    |-- get_latest_csv_dataset_from_s3
    |-- get_latest_parquet_dataset_from_s3
    |-- put_csv_dataset_to_s3
    |-- put_parquet_dataset_to_s3
    |-- Model
    |-- get_latest_pkl_model_from_s3
|-- logging
    |-- configure_logger
```

## AWS

A simple dataset and model management framework built on S3 object storage.

### Datsets

Training data files in CSV or Parquet format are saved to a S3 bucket using filenames with an ISO timestamp component:

```text
my-s3-project-bucket/
|
|-- datasets/
|    |-- ... 
|    |-- dataset_file_2021-07-10T07:42:23.csv
|    |-- dataset_file_2021-07-11T07:45:12.csv
|    |-- dataset_file_2021-07-12T07:41:02.csv
```

You can use `put_csv_dataset_to_s3` to persist a Pandas DataFrame directly to S3 with a compatible filename, or handle this yourself independently. The latest training data file can be retrieved using `get_latest_csv_dataset_from_s3`, which will return a `Dataset` object, which is an object with the following fields:

```python
class Dataset(NamedTuple):
    """Container for downloaded datasets and associated metadata."""

    data: DataFrame
    datetime: datetime
    bucket: str
    key: str
    hash: str
```

AWS S3 will compute the MD5 hash of every object uploaded to it (referred to as its Entity Tag). This is retrieved from S3 together with other basic metadata about the object. For example,

```python
get_latest_csv_dataset_from_s3("my-s3-project-bucket", "datasets")
# Dataset(
#     data=...,
#     datetime(2021, 7, 12, 7, 41, 02),
#     bucket="my-s3-project-bucket"),
#     key="datasets/dataset_file_2021-07-12T07:41:02.csv",
#     hash="759eccda4ceb7a07cda66ad4ef7cdfbc"
# )
```

This, together with S3 object versioning (if enabled), can be used to track the precise dataset used to train a model.

## Models

The `Model` class is a simple wrapper for a ML model that adds basic model metadata and the ability to serialise the model directly to S3. It requires a `Dataset` object containing the data used train the model, so that the model artefact can be explicitly linked to the precise version of the data used to train it. For example,

```python
from sklearn.tree import DecisionTreeRegressor


dataset = get_latest_csv_dataset_from_s3("my-s3-project-bucket", "datasets")
model = Model("my-model", DecisionTreeRegressor(), dataset, {"features": ["x1", "x2"], "foo": "bar"})

model
# name: my-model
# model_type: <class 'sklearn.tree._classes.DecisionTreeRegressor'>
# model_timestamp: 2021-07-12 07:46:08
# model_hash: ab6f998e0f5d8829fcb0017819c45020
# train_dataset_key: datasets/dataset_file_2021-07-12T07:41:02.csv
# train_dataset_hash: 759eccda4ceb7a07cda66ad4ef7cdfbc
# pipeline_git_commit_hash: e585fd3
```

Model objects can be directly serialised to S3,

```python
model.put_model_to_s3("my-s3-project-bucket", "models")
```

Which will create objects in a S3 bucket as follows,

```text
my-s3-project-bucket/
|
|-- models/
|    |-- ... 
|    |-- serialised_model_2021-07-10T07:47:33.pkl
|    |-- serialised_model_2021-07-11T07:49:14.pkl
|    |-- serialised_model_2021-07-12T07:46:08.pkl
```

The `Model` class is intended as a base class, suitable for pickle-able models (e.g. from Scikit-Learn). More complex model types (e.g. PyTorch or PyMC3 models) should inherit from `Model` and override the appropriate methods.

## Logging

The `configure_logger` function returns a Python logger configures to print logs using the Bodywork log format. For example,

```python
log = configure_logger()
log.into("foo")
# 2021-07-14 07:57:10,854 - INFO - pipeline.train - foo
```
