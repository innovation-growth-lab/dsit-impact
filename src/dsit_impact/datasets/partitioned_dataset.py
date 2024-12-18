"""
This module contains a custom PartitionedDataset that defaults to an empty DataFrame
with a predefined schema if no partitions are found.
"""
from typing import Callable, Any
import pandas as pd
from kedro.io import DatasetError
from kedro_datasets.partitions import PartitionedDataset


class DefaultablePartitionedDataset(PartitionedDataset):
    """
    A custom PartitionedDataset that defaults to an empty DataFrame
    with a predefined schema if no partitions are found.
    """

    def __init__(self, *args, schema: list[str] = None, **kwargs):
        """
        Initialises the dataset.

        Args:
            schema (list[str]): List of column names for the default empty DataFrame.
        """
        super().__init__(*args, **kwargs)
        self._schema = schema or []

    def load(self) -> dict[str, Callable[[], Any]]:
        try:
            partitions = super().load()
        except DatasetError:
            # Return an empty dataset if no partitions are found
            if self._schema:
                return {"default": lambda: pd.DataFrame(columns=self._schema)}
            raise
        return partitions
