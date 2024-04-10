from aiohttp import ClientSession
from datetime import (
    datetime,
    UTC,
)

from item import BucketInfo, MethodHttpRequest, ConfigUrl, ConfigToken, Token
import xmltodict

from session import aiohttp_session_decorator


class Header:

    def __init__(
        self,
        *,
        authorization: Token,
        x_amz_date: datetime = None,
        x_amz_algorithm="AWS4-HMAC-SHA256",
        content_type=None,
        content_disposition=None,
    ) -> None:

        headers = {}

        headers["X-Amz-Algorithm"] = x_amz_algorithm

        headers["Authorization"] = authorization

        if x_amz_date is None:
            headers["X-Amz-Date"] = datetime.now(UTC).strftime(
                "%Y%m%dT%H%M%SZ"
            )
        else:
            headers["X-Amz-Date"] = x_amz_date.strftime("%Y%m%dT%H%M%SZ")

        headers["Content-Type"] = content_type
        headers["Content-Disposition"] = content_disposition

        self.headers = headers

    def __call__(self):
        return dict(filter(lambda a: a[1] is not None, self.headers.items()))


class MinioAsyncClient:
    """Принимает использование асинхронной сессии

    client = MinioAsyncClient(
        access_key=`отпечаток`,
        secret_key=`секретный отпечаток`,
        host=`точка входа`,
        region=`регион установленый в Minio`,
        secure=`использовать SSL протокола`,
    )

    """

    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        host: str,
        region: str,
        secure: bool = True,
    ) -> None:
        self.__config = ConfigToken(
            service="s3",
            host=host,
            access_key=access_key,
            secret_access_key=secret_key,
            region_name=region,
        )

        self.__token = Token(self.__config)

        self.secure = secure

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def buckets_info(
        self, bucket_name: str, *, session: ClientSession = None
    ) -> BucketInfo:

        url = ConfigUrl(
            MethodHttpRequest.GET, self.__config.host, f"/{bucket_name}/"
        )

        headers = Header(authorization=self.__token(url))

        async with session.get(str(url), headers=headers()) as response:
            request_text = await response.text()

            request_text_dict = xmltodict.parse(request_text)

            if response.status == 200:
                return BucketInfo(request_text_dict)
            else:
                raise

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def buckets_exsist(
        self, bucket_name: str, *, session: ClientSession = None
    ) -> BucketInfo:
        url = ConfigUrl(
            MethodHttpRequest.GET, self.__config.host, f"/{bucket_name}/", {}
        )

        headers = Header(authorization=self.__token(url))

        async with session.get(str(url), headers=headers()) as response:
            if response.status == 200:
                return True
            elif response.status == 404:
                return False
            else:
                return False

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def buckets_create(
        self, bucket_name: str, *, session: ClientSession = None
    ):
        url = ConfigUrl(
            MethodHttpRequest.PUT, self.__config.host, f"/{bucket_name}/", {}
        )

        headers = Header(
            authorization=self.__token(url),
        )

        async with session.put(str(url), headers=headers()) as response:
            print(response.status)
            print(await response.text())

            if response.status == 200:
                return True
            elif response.status == 404:
                return False
            else:
                return False

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def buckets_delete(
        self, bucket_name: str, *, session: ClientSession = None
    ):

        url = ConfigUrl(
            MethodHttpRequest.DELETE,
            self.__config.host,
            f"/{bucket_name}/",
            {},
        )

        headers = Header(
            authorization=self.__token(url),
        )

        async with session.delete(str(url), headers=headers()) as response:
            print(response.status)
            print(await response.text())

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def buckets_list_info(self, *, session: ClientSession = None):

        url = ConfigUrl(MethodHttpRequest.GET, self.__config.host, "/", {})

        headers = Header(
            authorization=self.__token(url),
        )

        async with session.get(str(url), headers=headers()) as response:
            print(response.status)

            request_text_dict = xmltodict.parse(await response.text())
            buckets = request_text_dict["ListAllMyBucketsResult"]["Buckets"][
                "Bucket"
            ]

            return [
                await self.buckets_info(bucket["Name"]) for bucket in buckets
            ]

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def buckets_list_name(self, *, session: ClientSession = None):

        url = ConfigUrl(MethodHttpRequest.GET, self.__config.host, "/", {})

        headers = Header(
            authorization=self.__token(url),
        )

        async with session.get(str(url), headers=headers()) as response:
            print(response.status)

            request_text_dict = xmltodict.parse(await response.text())
            buckets = request_text_dict["ListAllMyBucketsResult"]["Buckets"][
                "Bucket"
            ]

            return [bucket["Name"] for bucket in buckets]

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def download_file(
        self,
        bucket_name: str,
        file_name: str,
        *,
        session: ClientSession = None,
    ):
        # encoded_file_name = quote(file_name)
        url = ConfigUrl(
            MethodHttpRequest.GET,
            self.__config.host,
            f"/{bucket_name}/{file_name}",
        )

        headers = Header(authorization=self.__token(url))

        async with session.get(url(), headers=headers()) as response:
            return await response.content.read()

    @aiohttp_session_decorator  # TODO: Добавить exception
    async def upload_file(
        self,
        bucket_name: str,
        file_name: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        *,
        session: ClientSession = None,
    ):

        url = ConfigUrl(
            MethodHttpRequest.PUT,
            self.__config.host,
            f"/{bucket_name}/{file_name}",
        )

        headers = Header(
            authorization=self.__token(url), content_type=content_type
        )

        async with session.put(
            url(), headers=headers(), data=content
        ) as response:
            ...
