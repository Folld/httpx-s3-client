import os
from typing import Optional

import pytest
from httpx import URL, AsyncClient

from httpx_s3_client import S3Client


@pytest.fixture
async def s3_client(s3_url: URL, s3_access_key_id: str, s3_secret_access_key: str):
    client = AsyncClient()
    yield S3Client(client=client, url=s3_url, region="us-east-1", access_key_id=s3_access_key_id,
                   secret_access_key=s3_secret_access_key)


@pytest.fixture
def s3_url() -> URL:
    return URL(os.getenv("S3_URL", "http://user:hackme@localhost:8000/"))


@pytest.fixture
def s3_access_key_id() -> str:
    return os.getenv("S3_ACCESS_KEY", "test")


@pytest.fixture
def s3_secret_access_key() -> str:
    return os.getenv("S3_SECRET_ACCESS_KEY", "test")


@pytest.fixture
def s3_bucket_name() -> str:
    return os.getenv("S3_BUCKET", "test")


@pytest.fixture
def object_name(s3_bucket_name) -> str:
    return f"/{s3_bucket_name}/test"


@pytest.fixture
def s3_read(s3_client: S3Client, object_name):
    async def do_read(custom_object_name: Optional[str] = None):
        s3_key = custom_object_name or object_name
        return await s3_client.get(s3_key)

    return do_read
