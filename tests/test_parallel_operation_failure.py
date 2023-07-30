from http import HTTPStatus

import pytest
from httpx import URL, Request
from pytest_httpx import HTTPXMock

from httpx_s3_client.client import AwsDownloadError, AwsUploadError


@pytest.mark.timeout(1)
async def test_multipart_upload_failure(s3_client, s3_mocker):
    s3_mocker.s3_mock_post_object_handler()
    s3_mocker.s3_mock_put_object_handler()
    def iterable():
        for _ in range(8):  # type: int
            yield b"hello world" * 1024

    with pytest.raises(AwsUploadError):
        await s3_client.put_multipart(
            "/test/test",
            iterable(),
            workers_count=4,
            part_upload_tries=3,
        )


async def test_parallel_download_failure(s3_client, s3_mocker, tmpdir):
    s3_mocker.s3_mock_head_object_handler()
    s3_mocker.s3_mock_get_object_handler()
    with pytest.raises(AwsDownloadError):
        await s3_client.get_file_parallel(
            "foo/bar.txt",
            tmpdir / "bar.txt",
            workers_count=4,
        )


@pytest.fixture
def s3_url(s3_mock_url):
    return s3_mock_url


CREATE_MP_UPLOAD_RESPONSE = """\
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>
</InitiateMultipartUploadResult>
"""


@pytest.fixture
def s3_mock_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


@pytest.fixture
def s3_mock_url(s3_mock_port, localhost):
    return URL(
        scheme="http",
        host=localhost,
        port=s3_mock_port,
        username="user",
        password="hackme",
    )


@pytest.fixture
def s3_mocker(httpx_mock):
    class Mocker:
        def __init__(self, _httpx_mock: HTTPXMock):
            self._httpx_mock = httpx_mock

        def s3_mock_put_object_handler(self):
            self._httpx_mock.add_response(
                method="PUT",
                content=b"",
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        def s3_mock_post_object_handler(self):
            def custom_response(request: Request):
                content = b""
                if "uploads" in request.url.params:
                    content = CREATE_MP_UPLOAD_RESPONSE
                self._httpx_mock.add_response(
                    method="POST",
                    content=content
                )

            httpx_mock.add_callback(custom_response)

        def s3_mock_head_object_handler(self):
            self._httpx_mock.add_response(
                method="HEAD",
                headers={
                    "Content-length": str((1024 ** 2) * 16),
                    "Etag": "7e10e7d25dc4581d89b9285be5f384fd",
                },
            )

        def s3_mock_get_object_handler(self):
            self._httpx_mock.add_response(
                method="GET",
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                headers={
                    "Content-length": str((1024 ** 2) * 5),
                    "Etag": "7e10e7d25dc4581d89b9285be5f384fd",
                },
            )
    return Mocker(httpx_mock)
