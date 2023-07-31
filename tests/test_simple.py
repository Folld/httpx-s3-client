import sys
from http import HTTPStatus
from pathlib import Path

import pytest

from httpx_s3_client import S3Client


@pytest.mark.parametrize("object_name", ("{bucket}/test", "/{bucket}/test"))
async def test_put_str(s3_client: S3Client, object_name, s3_bucket_name):
    object_name = object_name.format(bucket=s3_bucket_name)
    data = "hello, world"
    resp = await s3_client.put(object_name, data)
    assert resp.status_code == HTTPStatus.OK

    resp = await s3_client.get(object_name)
    result = resp.content.decode()
    assert result == data


@pytest.mark.parametrize("object_name", ("{bucket}/test", "/{bucket}/test"))
async def test_put_bytes(s3_client: S3Client, s3_read, object_name, s3_bucket_name):
    data = b"hello, world"
    object_name = object_name.format(bucket=s3_bucket_name)
    resp = await s3_client.put(object_name, data)
    assert resp.status_code == HTTPStatus.OK
    assert (await s3_read(object_name)).content == data


@pytest.mark.parametrize("object_name", ("{bucket}/test", "/{bucket}/test"))
async def test_put_async_iterable(s3_client: S3Client, s3_read, object_name, s3_bucket_name):
    async def async_iterable(iterable: bytes):
        for i in iterable:
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    object_name = object_name.format(bucket=s3_bucket_name)
    resp = await s3_client.put(object_name, async_iterable(data))
    assert resp.status_code == HTTPStatus.OK

    assert (await s3_read(object_name)).content == data


async def test_put_file(s3_client: S3Client, s3_read, tmp_path, s3_bucket_name):
    data = b"hello, world"
    object_name = f"{s3_bucket_name}/test"
    object_name_2 = object_name + '2'

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        # Test upload by file str path
        resp = await s3_client.put_file(object_name, f.name)
        assert resp.status_code == HTTPStatus.OK

        assert (await s3_read(object_name)).content == data

        # Test upload by file Path
        resp = await s3_client.put_file(object_name_2, Path(f.name))
        assert resp.status_code == HTTPStatus.OK

        assert (await s3_read(object_name_2)).content == data


async def test_list_objects_v2(s3_client: S3Client, s3_read, tmp_path, s3_bucket_name):
    data = b"hello, world"
    object_name = f"{s3_bucket_name}/list/test"

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        resp = await s3_client.put_file(object_name + '1', f.name)
        assert resp.status_code == HTTPStatus.OK

        resp = await s3_client.put_file(object_name + '2', f.name)
        assert resp.status_code == HTTPStatus.OK

        # Test list file
        batch = 0
        async for result in s3_client.list_objects_v2(
                prefix=f"{s3_bucket_name}/list",
                delimiter="/",
                max_keys=1,
        ):
            batch += 1
            assert result[0].key == f"{object_name}{batch}"
            assert result[0].size == len(data)


async def test_url_path_with_colon(s3_client: S3Client, s3_read, s3_bucket_name):
    data = b"hello, world"
    key = f"{s3_bucket_name}/some-path:with-colon.txt"
    resp = await s3_client.put(key, data)
    assert resp.status_code == HTTPStatus.OK

    assert (await s3_read(key)).content == data


@pytest.mark.parametrize("object_name", ("{bucket}/test", "/{bucket}/test"))
async def test_put_compression(s3_client: S3Client, s3_read, object_name, s3_bucket_name):
    async def async_iterable(iterable: bytes):
        for i in iterable:
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    object_name = object_name.format(bucket=s3_bucket_name)
    resp = await s3_client.put(
        object_name, async_iterable(data), headers={'Accept-Encoding': 'gzip'},
    )
    assert resp.status_code == HTTPStatus.OK

    result = await s3_read(object_name)
    # assert resp.headers[hdrs.CONTENT_ENCODING] == "gzip"
    # FIXME: uncomment after update fakes3 image
    assert data == result.content
