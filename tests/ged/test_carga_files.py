import io
from contextlib import contextmanager

from dtb_iop_raw_elt.ged.carga_files import chunk_ids, copy_one_file
from dtb_iop_raw_elt.ged.diff_pathfiles import SmbCredentials


def test_chunk_ids_splits_into_batches_of_given_size():
    assert chunk_ids([1, 2, 3, 4, 5], batch_size=2) == [[1, 2], [3, 4], [5]]


def test_chunk_ids_empty_list():
    assert chunk_ids([], batch_size=2) == []


def test_chunk_ids_single_batch_when_smaller_than_batch_size():
    assert chunk_ids([1, 2], batch_size=5) == [[1, 2]]


@contextmanager
def _fake_open_file(content: bytes):
    yield io.BytesIO(content)


def test_copy_one_file_writes_bytes_and_reports_success(tmp_path):
    creds = SmbCredentials(user="u", password="p", server_file_path="\\\\server\\share\\GED")

    def open_file(path, mode, username, password):
        assert path == "\\\\server\\share\\GED\\a.pdf"
        assert username == "u"
        assert password == "p"
        return _fake_open_file(b"conteudo")

    result = copy_one_file(
        open_file, str(tmp_path), "a.pdf", "\\\\server\\share\\GED\\a.pdf", creds
    )

    assert result == {"success": True, "error_message": None}
    assert (tmp_path / "a.pdf").read_bytes() == b"conteudo"


def test_copy_one_file_reports_failure_without_raising(tmp_path):
    creds = SmbCredentials(user="u", password="p", server_file_path="\\\\server\\share\\GED")

    def open_file(*args, **kwargs):
        raise OSError("conexão recusada")

    result = copy_one_file(
        open_file, str(tmp_path), "a.pdf", "\\\\server\\share\\GED\\a.pdf", creds
    )

    assert result["success"] is False
    assert "conexão recusada" in result["error_message"]
