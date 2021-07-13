"""
Cass and functions for managing ML models.
"""
from datetime import datetime
from hashlib import md5
from os import environ
from pickle import dump, dumps, PicklingError
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional

from bodywork_pipeline_utils.aws.datasets import Dataset
from bodywork_pipeline_utils.aws.s3 import make_timestamped_filename, put_file_to_s3


class Model:
    def __init__(
        self,
        name: str,
        model: Any,
        train_dataset: Dataset,
        metadata: Optional[Dict[str, Any]] = None,
    ):
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
        info = (
            f"name: {self._name}"
            f"model_type: {self._model_type}"
            f"model_timestamp: {self._creation_time}"
            f"model_hash: {self._model_hash}"
            f"train_dataset_key: {self._train_dataset_key}"
            f"train_dataset_hash: {self._train_dataset_hash}"
            f"pipeline_git_commit_hash: {self._pipeline_git_commit_hash}"
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

    def put_model_to_s3(self, bucket: str, folder: str = "") -> None:
        """Upload model to S3 as a pickle file.

        Args:
            bucket: Location on S3 to persist the data.
            folder: Folder within the bucket, defaults to "".
        """
        filename = make_timestamped_filename(self._name, self._creation_time, "pkl")
        with NamedTemporaryFile() as temp_file:
            dump(self, temp_file, protocol=5)
            put_file_to_s3(temp_file.name, bucket, folder, filename)
