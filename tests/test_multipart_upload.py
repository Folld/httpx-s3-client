import pytest

from httpx_s3_client import S3Client


async def test_multipart_file_upload(s3_client: S3Client, s3_read, tmp_path, s3_bucket_name):
    data = b"hello, world" * 1024 * 128

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        await s3_client.put_file_multipart(
            f"/{s3_bucket_name}/test_multipart",
            f.name,
            part_size=5 * (1024 * 1024),
        )

    assert data == (await s3_read(f"/{s3_bucket_name}/test_multipart")).content


@pytest.mark.parametrize("calculate_content_sha256", [True, False])
@pytest.mark.parametrize("workers_count", [1, 2])
async def test_multipart_stream_upload(
    calculate_content_sha256, workers_count,
    s3_client: S3Client, s3_read, tmp_path, s3_bucket_name
):

    def iterable():
        for _ in range(8):  # type: int
            yield b"hello world" * 1024 * 1024

    await s3_client.put_multipart(
        f"/{s3_bucket_name}/test",
        iterable(),
        calculate_content_sha256=calculate_content_sha256,
        workers_count=workers_count,
    )

    assert (await s3_read(f"/{s3_bucket_name}/test")).content == b"hello world" * 1024 * 1024 * 8
