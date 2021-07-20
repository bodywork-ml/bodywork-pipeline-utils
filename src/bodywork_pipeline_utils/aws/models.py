"""
Cass and functions for managing ML models.
"""
from datetime import datetime
from hashlib import md5
from os import environ
from pickle import dump, dumps, loads, PicklingError, UnpicklingError
from tempfile import NamedTemporaryFile
from typing import Any, cast, Dict, Optional

from bodywork_pipeline_utils.aws.datasets import Dataset
from bodywork_pipeline_utils.aws.artefacts import (
    find_latest_artefact_on_s3,
    make_timestamped_filename,
    put_file_to_s3,
)


class Model:
    """Base class for representing ML models and metadata."""

    def __init__(
        self,
        name: str,
        model: Any,
        train_dataset: Dataset,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Constructor.

        Args:
            name: Model name.
            model: Trained model object.
            train_dataset: Dataset object used to train the model.
            metadata: Arbitrary model metadata.
        """
        self._name = name
        self._train_dataset_key = train_dataset.key
        self._train_dataset_hash = train_dataset.hash
        self._model_hash = self._compute_model_hash(model)
        self._model = model
        self._model_type = type(model)
        self._creation_time = datetime.now()
        self._pipeline_git_commit_hash = environ.get("GIT_COMMIT_HASH", "NA")
        self._metadata = metadata

    def __eq__(self, other: object) -> bool:
        """Model quality operator."""
        if isinstance(other, Model):
            conditions = [
                self._train_dataset_hash == other._train_dataset_hash,
                self._train_dataset_key == other._train_dataset_key,
                self._creation_time == other._creation_time,
                self._pipeline_git_commit_hash == other._pipeline_git_commit_hash,
            ]
            if all(conditions):
                return True
            else:
                return False
        else:
            return False

    def __repr__(self) -> str:
        """Stdout representation."""
        info = (
            f"name: {self._name}\n"
            f"model_type: {self._model_type}\n"
            f"model_timestamp: {self._creation_time}\n"
            f"model_hash: {self._model_hash}\n"
            f"train_dataset_key: {self._train_dataset_key}\n"
            f"train_dataset_hash: {self._train_dataset_hash}\n"
            f"pipeline_git_commit_hash: {self._pipeline_git_commit_hash}"
        )
        return info

    def __str__(self) -> str:
        """String representation."""
        info = (
            f"name:{self._name}|"
            f"model_type:{self._model_type}|"
            f"model_timestamp:{self._creation_time}|"
            f"model_hash:{self._model_hash}|"
            f"train_dataset_key:{self._train_dataset_key}|"
            f"train_dataset_hash:{self._train_dataset_hash}|"
            f"pipeline_git_commit_hash:{self._pipeline_git_commit_hash}"
        )
        return info

    @property
    def metadata(self) -> Optional[Dict[str, Any]]:
        return self._metadata

    @property
    def model(self) -> Any:
        return self._model

    @staticmethod
    def _compute_model_hash(model: Any) -> str:
        """Compute a hash for a model object."""
        try:
            model_bytestream = dumps(model, protocol=5)
            hash = md5(model_bytestream)
            return hash.hexdigest()
        except PicklingError:
            msg = "Could not pickle model into bytes before hashing."
            raise RuntimeError(msg)
        except Exception as e:
            msg = "Could not hash model."
            raise RuntimeError(msg) from e

    def put_model_to_s3(self, bucket: str, folder: str = "") -> str:
        """Upload model to S3 as a pickle file.

        Args:
            bucket: Location on S3 to persist the data.
            folder: Folder within the bucket, defaults to "".
        """
        filename = make_timestamped_filename(self._name, self._creation_time, "pkl")
        with NamedTemporaryFile() as temp_file:
            dump(self, temp_file, protocol=5)
            put_file_to_s3(temp_file.name, bucket, folder, filename)
        return f"{bucket}/{folder}/{filename}"


def get_latest_pkl_model_from_s3(bucket: str, folder: str = "") -> Model:
    """Get the latest model from S3.

    Args:
        bucket: S3 bucket to look in.
        folder: Folder within bucket to limit search, defaults to "".

    Returns:
        Dataset object.
    """
    artefact = find_latest_artefact_on_s3("pkl", bucket, folder)
    try:
        artefact_bytes = artefact.get().read()
        model = cast(Model, loads(artefact_bytes))
        return model
    except UnpicklingError:
        msg = "artefact at {bucket}/{model.obj_key} could not be unpickled."
        raise RuntimeError(msg)
    except AttributeError:
        msg = "artefact at {bucket}/{model.obj_key} is not type Model."
        raise RuntimeError(msg)
