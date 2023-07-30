import datetime
import os
from unittest import mock

import pytest
from httpx import AsyncClient
from pytest import FixtureRequest
from pytest_httpx import HTTPXMock

from httpx_s3_client.credentials import (
    AbstractCredentials, ConfigCredentials, EnvironmentCredentials,
    MetadataCredentials, StaticCredentials, URLCredentials, merge_credentials,
)


def test_static_credentials():
    assert not StaticCredentials(access_key_id="", secret_access_key="")
    assert StaticCredentials(access_key_id="foo", secret_access_key="bar")


def test_static_credentials_repr():
    assert "hack-me" not in repr(
        StaticCredentials(access_key_id="foo", secret_access_key="hack-me"),
    )


def test_url_credentials():
    credentials = URLCredentials("http://key:secret@host")
    assert isinstance(credentials, AbstractCredentials)
    assert credentials.signer.access_key_id == "key"
    assert credentials.signer.secret_access_key == "secret"


@mock.patch.dict(os.environ, {})
def test_env_credentials_false():
    assert not EnvironmentCredentials()


@mock.patch.dict(
    os.environ, {
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "hack-me",
        "AWS_DEFAULT_REGION": "cc-mid-2",
    },
)
def test_env_credentials_mock():
    cred = EnvironmentCredentials()
    assert isinstance(cred, AbstractCredentials)
    assert cred.access_key_id == "key"
    assert cred.secret_access_key == "hack-me"
    assert cred.region == "cc-mid-2"


def test_config_credentials(tmp_path):
    with open(tmp_path / "credentials", "w") as fp:
        fp.write(
            "\n".join([
                "[test-profile]",
                "  aws_access_key_id = test-key",
                "  aws_secret_access_key = test-secret",
                "[default]",
                "aws_access_key_id = default-key",
                "aws_secret_access_key = default-secret",
            ]),
        )

    with open(tmp_path / "config", "w") as fp:
        fp.write(
            "\n".join([
                "[test-profile]",
                "  region = ru-central1",
                "[default]",
                "region = us-east-1",
            ]),
        )

    cred = ConfigCredentials(
        credentials_path=tmp_path / "credentials",
        config_path=tmp_path / "config",
    )
    assert isinstance(cred, AbstractCredentials)
    assert cred
    assert cred.access_key_id == "default-key"
    assert cred.secret_access_key == "default-secret"

    cred = ConfigCredentials(
        credentials_path=tmp_path / "credentials",
        config_path=tmp_path / "config",
        profile="test-profile",
    )
    assert isinstance(cred, AbstractCredentials)
    assert cred
    assert cred.access_key_id == "test-key"
    assert cred.secret_access_key == "test-secret"


@pytest.fixture
def metadata_server_app(httpx_mock: HTTPXMock):
    class Mocker:
        def __init__(self, _httpx_mock: HTTPXMock):
            self._httpx_mock = _httpx_mock
            self._base_url = 'http://localhost'

        def set_base_url(self, base_url: str):
            self._base_url = base_url.rstrip('/')

        def mock_get_iam_role(self):
            """
            $ curl -v \
                169.254.169.254/latest/meta-data/iam/security-credentials/myiamrole
            *   Trying 169.254.169.254:80...
            * Connected to 169.254.169.254 (169.254.169.254) port 80 (#0)
            > GET /latest/meta-data/iam/security-credentials/myiamrole HTTP/1.1
            > Host: 169.254.169.254
            > User-Agent: curl/7.87.0
            > Accept: */*
            >
            * Mark bundle as not supporting multiuse
            < HTTP/1.1 200 OK
            < Content-Type: text/plain
            < Accept-Ranges: none
            < Last-Modified: Fri, 30 Jun 2023 19:06:40 GMT
            < Content-Length: 1582
            < Date: Fri, 30 Jun 2023 20:00:47 GMT
            < Server: EC2ws
            < Connection: close
            <
            {
              "Code" : "Success",
              "LastUpdated" : "2023-06-30T19:06:42Z",
              "Type" : "AWS-HMAC",
              "AccessKeyId" : "ANOTATOKEN5345W4RX",
              "SecretAccessKey" : "VGJvJ5H34NOTATOKENAJikpQN/Riq",
              "Token" : "INOTATOKENQoJb3JpZ2luX2V...SzrAFy",
              "Expiration" : "2023-07-01T01:25:35Z"
            }

            """
            last_updated = datetime.datetime.utcnow()

            self._httpx_mock.add_response(
                method='GET',
                url=self._base_url + "/latest/meta-data/iam/security-credentials/pytest",
                json={
                    "Code": "Success",
                    "LastUpdated": last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Type": "AWS-HMAC",
                    "AccessKeyId": "PYTESTACCESSKEYID",
                    "SecretAccessKey": "PYTESTACCESSKEYSECRET",
                    "Token": "PYTESTACCESSTOKEN",
                    "Expiration": (
                        last_updated + datetime.timedelta(hours=2)
                    ).strftime(
                        "%Y-%m-%dT%H:%M:%SZ",
                    ),
                },
            )

        def mock_get_security_credentials(self):
            """
            GET /latest/meta-data/iam/security-credentials/ HTTP/1.1
            > Host: 169.254.169.254
            > User-Agent: curl/7.87.0
            > Accept: */*
            >
            * Mark bundle as not supporting multiuse
            < HTTP/1.1 200 OK
            < Content-Type: text/plain
            < Accept-Ranges: none
            < Last-Modified: Fri, 30 Jun 2023 19:06:40 GMT
            < Content-Length: 14
            < Date: Fri, 30 Jun 2023 20:02:18 GMT
            < Server: EC2ws
            < Connection: close
            <
            pytest
            """
            self._httpx_mock.add_response(
                method="GET",
                url=f"{self._base_url}/latest/meta-data/iam/security-credentials/",
                content=b"pytest",
            )

        def mock_get_instance_identity(self):
            """
            $ curl-v \
                http://169.254.169.254/latest/dynamic/instance-identity/document
            *   Trying 169.254.169.254:80...
            * Connected to 169.254.169.254 (169.254.169.254) port 80 (#0)
            > GET /latest/dynamic/instance-identity/document HTTP/1.1
            > Host: 169.254.169.254
            > User-Agent: curl/7.87.0
            > Accept: */*
            >
            * Mark bundle as not supporting multiuse
            < HTTP/1.1 200 OK
            < Content-Type: text/plain
            < Accept-Ranges: none
            < Last-Modified: Fri, 30 Jun 2023 19:06:40 GMT
            < Content-Length: 478
            < Date: Fri, 30 Jun 2023 20:03:14 GMT
            < Server: EC2ws
            < Connection: close
            <
            {
              "accountId" : "123123",
              "architecture" : "x86_64",
              "availabilityZone" : "us-east-1a",
              "billingProducts" : null,
              "devpayProductCodes" : null,
              "marketplaceProductCodes" : null,
              "imageId" : "ami-123123",
              "instanceId" : "i-11232323",
              "instanceType" : "t3a.micro",
              "kernelId" : null,
              "pendingTime" : "2023-06-13T18:18:58Z",
              "privateIp" : "172.33.33.33",
              "ramdiskId" : null,
              "region" : "us-east-1",
              "version" : "2017-09-30"
            }
            """

            self._httpx_mock.add_response(
                method='GET',
                url=f'{self._base_url}/latest/dynamic/instance-identity/document',
                json={
                    "accountId": "123123",
                    "architecture": "x86_64",
                    "availabilityZone": "us-east-1a",
                    "billingProducts": None,
                    "devpayProductCodes": None,
                    "marketplaceProductCodes": None,
                    "imageId": "ami-123123",
                    "instanceId": "i-11232323",
                    "instanceType": "t3a.micro",
                    "kernelId": None,
                    "pendingTime": "2023-06-13T18:18:58Z",
                    "privateIp": "172.33.33.33",
                    "ramdiskId": None,
                    "region": "us-east-99",
                    "version": "2017-09-30",
                },
            )
    return Mocker(httpx_mock)


async def test_metadata_credentials(
    request: FixtureRequest,
    metadata_server_app,
):
    server = AsyncClient(base_url='http://localhost')
    metadata_server_app.set_base_url(str(server.base_url))
    metadata_server_app.mock_get_instance_identity()
    metadata_server_app.mock_get_iam_role()
    metadata_server_app.mock_get_security_credentials()

    class TestMetadataCredentials(MetadataCredentials):
        METADATA_ADDRESS = server.base_url.host

    credentials = TestMetadataCredentials()
    assert isinstance(credentials, AbstractCredentials)

    assert not credentials

    with pytest.raises(RuntimeError):
        print(credentials.signer)

    await credentials.start()
    request.addfinalizer(credentials.stop)

    assert credentials

    assert credentials.signer
    assert credentials.signer.region == "us-east-99"
    assert credentials.signer.access_key_id == "PYTESTACCESSKEYID"
    assert credentials.signer.secret_access_key == "PYTESTACCESSKEYSECRET"
    assert credentials.signer.session_token == "PYTESTACCESSTOKEN"


async def test_merge_credentials():
    credentials = [
        StaticCredentials(access_key_id="access_key"),
        StaticCredentials(secret_access_key="secret"),
        StaticCredentials(session_token="token")
    ]

    assert not all(credentials)

    result = merge_credentials(*credentials)
    assert result

    assert result.access_key_id == credentials[0].access_key_id
    assert result.secret_access_key == credentials[1].secret_access_key
    assert result.session_token == credentials[2].session_token

    result = merge_credentials(
        StaticCredentials(
            access_key_id="overriden",
            secret_access_key="overriden",
            session_token="overriden"
        ),
        *credentials
    )

    assert result.access_key_id == "overriden"
    assert result.secret_access_key == "overriden"
    assert result.session_token == "overriden"
