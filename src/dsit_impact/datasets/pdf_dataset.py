import io
from pathlib import PurePosixPath
from typing import Any, Dict, Union, Sequence
from kedro.io import AbstractDataset
from kedro.io.core import get_filepath_str, get_protocol_and_path
import fsspec
import pymupdf
import logging

log = logging.getLogger(__name__)

class PdfDataset(AbstractDataset):
    """
    A dataset class for working with PDF files.

    Args:
        filepath (str): The path to the PDF file.
        credentials (Dict[str, Any], optional): Credentials for accessing the file system. Defaults to None.
    """

    def __init__(self, filepath: str, credentials: Dict[str, Any] = None):
        protocol, path = get_protocol_and_path(filepath)
        self._protocol = protocol
        self._filepath = PurePosixPath(path)
        self._fs = fsspec.filesystem(self._protocol, **credentials)

    def _load(self) -> pymupdf.Document:
        """
        Load the PDF file and return a `pymupdf.Document` object.

        Returns:
            pymupdf.Document: The loaded PDF document.
        """
        load_path = get_filepath_str(str(self._filepath), self._protocol)
        with self._fs.open(load_path, "rb") as f:
            pdf = pymupdf.open(f)
        return pdf

    def _save(self, data: io.BytesIO) -> None:
        """
        Save the PDF data to the file system.

        Args:
            data (io.BytesIO): The PDF data to be saved.
        """
        if isinstance(data, list):
            data = [(filename, pdf) for filename, pdf in data if isinstance(pdf, bytes)]
            for pdf in data:
                filename, pdf_object = pdf
                if not filename.endswith(".pdf"):
                    filename = "".join([filename, ".pdf"])
                inner_filepath = PurePosixPath("".join([str(self._filepath), "/", filename]))
                save_path = get_filepath_str(inner_filepath, self._protocol)
                with self._fs.open(save_path, "wb") as f:
                    f.write(pdf_object)
        else:
            if data is None or isinstance(data, str):
                log.info("No data to save")
                return
            save_path = get_filepath_str(self._filepath, self._protocol)
            with self._fs.open(save_path, "wb") as f:
                f.write(data)

    def _describe(self) -> Dict[str, Any]:
        return dict(filepath=self._filepath)
    