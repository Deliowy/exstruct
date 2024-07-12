import asyncio
import itertools
import time
import typing
import urllib

import aiohttp
import more_itertools
import tenacity
import requests
import requests.auth
import requests.exceptions
from tenacity import retry

from ..util import _util
from ._base_parser import PARSER_BATCH_SIZE, BaseParser


class APIParser(BaseParser):
    """Parser for data-sources that provide data via API (REST or another)"""

    def __init__(self, source: str, response_type: str, **kwargs) -> None:
        """Parser for data-sources that provide data via API (REST or another)

        Args:
            source (str): URL to API
            response_type (str): Type of response from API (bytes, JSON, XML, plain text, etc.)

        KwArgs:
            user (str): Username to be used for authorization
            password (str): Password to be used for authorization
        """
        super().__init__(source, response_type, **kwargs)
        if self.user or self.password:
            self.auth = requests.auth.HTTPBasicAuth(self.user, self.password)
        else:
            self.auth = None

        self.retry_condition = kwargs.pop("retry_condition", API_retry_if_status_code())

    def parse(self, method: str, request_params: dict = None, *args, **kwargs):
        """Parse data from API

        Args:
            method (str): REST method (GET, POST)
            request_params (dict, optional): Request params to use in requests to API. Defaults to `None`.

        Raises:
            ValueError: Raised if `method` not supported

        Returns:
            Any: API response
        """
        if request_params:
            payload = request_params
        else:
            payload = [request_params]

        url = urllib.parse.urljoin(self.source, kwargs.pop("api_url", ""))
        with requests.Session() as session:
            session.headers.update(kwargs.pop("headers", {}))
            session.auth = self.auth
            recieved_data = []

            if method.upper() == "GET":
                recieved_data = self.get(session, url, payload)
            elif method.upper() == "POST":
                recieved_data = self.post(session, url, payload)
            else:
                err_msg = f"Method {method.upper()} is not supported"
                raise ValueError(err_msg)
        return recieved_data

    def parse_many(
        self,
        method: str,
        requests_params: typing.Iterable | typing.TextIO = None,
        use_params_product: bool = False,
        *args,
        **kwargs,
    ):
        """Parse data source using each set of request parameters and return total result

        Args:
            method (str): method used to retrieve data (`GET`, `POST`, etc.)
            requests_params (typing.Iterable | typing.TextIO, optional): Sets of request parameters to use. Defaults to `None`. Params format: `[(key,value1),(key,value2),...]`
            use_params_product (bool, optional): Check to use cartesian product of request parameters. Defaults to `False`.

        KwArgs:
            break_time (float): pause between requests to API. Defaults to `0`.
            batch_size (int): batch size of request parameters to be processed at a time. Defaults to `10_000`
            api_url (str): relative url to API. Defaults to `''`.
            headers (dict): headers to be used during interaction with API. Defaults to `{}`.

        Returns:
            list: list of request replies
        """
        self.break_time = kwargs.pop("break_time", 0)

        # TODO Описать поведение, если получаемый массив параметров дохуя большой
        if requests_params:
            requests_payloads = requests_params
        else:
            requests_payloads = [requests_params]

        batch_size = kwargs.pop("batch_size", PARSER_BATCH_SIZE)
        if not None in requests_payloads:
            payloads = self.prepare_payloads(
                requests_payloads,
                use_params_product=use_params_product,
                batch_size=batch_size,
            )
        else:
            payloads = [requests_payloads]

        url = urllib.parse.urljoin(self.source, kwargs.pop("api_url", ""))

        with requests.Session() as session:
            session.headers.update(kwargs.pop("headers", {}))
            session.auth = self.auth
            recieved_data = []

            if method.upper() == "GET":
                recieved_data = self.get_many(session, url, payloads)
            elif method.upper() == "POST":
                recieved_data = self.post_many(session, url, payloads)
            else:
                err_msg = f"Method {method.upper()} is not supported"
                raise ValueError(err_msg)

        return recieved_data

    @classmethod
    def prepare_payloads(
        cls,
        requests_payloads: typing.Iterable,
        use_params_product: bool,
        batch_size: int,
    ):
        """Prepare parameters for use in requests

        Args:
            requests_payloads (typing.Iterable): parameters to use in requests
            use_params_product (bool): Use cartesian product of request payloads
            batch_size (int): amount of payloads per batch

        Returns:
            Iterator[List[dict]]: payloads for requests
        """
        if use_params_product:
            requests_batches = itertools.product(*requests_payloads)
        else:
            requests_batches = requests_payloads

        requests_batches = map(dict, requests_batches)

        requests_batches = more_itertools.batched(
            requests_batches,
            batch_size,
        )
        return requests_batches

    def get_many(
        self,
        session: requests.Session,
        url: str,
        requests_batches: typing.Iterable,
    ):
        """Make requests to data source using `GET` with given payloads

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            requests_batches (typing.Iterable): requests payloads

        Returns:
            list: list of recieved responses
        """
        recieved_data = []
        for batch in requests_batches:
            time.sleep(self.break_time)
            for query_params in batch:
                query = (
                    urllib.parse.urlencode(query_params, doseq=True)
                    if any(query_params)
                    else None
                )
                recieved_data.append(self.fetch(session, "get", url, params=query))

        return recieved_data

    def get(
        self,
        session: requests.Session,
        url: str,
        query_params: dict,
    ):
        """Make request to data source using `GET` with given query parameters

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            query_params (dict): parameters to add to query url

        Returns:
            Any: request response
        """
        query = (
            urllib.parse.urlencode(query_params, doseq=True)
            if any(query_params)
            else None
        )
        recieved_data = self.fetch(session, "get", url, params=query)

        return recieved_data

    def post_many(
        self,
        session: requests.Session,
        url: str,
        requests_batches: typing.Iterable,
    ):
        """Make requests to data source using `POST` with given payloads

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            requests_batches (typing.Iterable): requests payloads

        Returns:
            list: list of recieved responses
        """
        results = []
        for batch in requests_batches:
            time.sleep(self.break_time)
            for payload in batch:
                results.append(self.fetch(session, "post", url, data=payload))
        return results

    def post(
        self,
        session: requests.Session,
        url: str,
        payload: typing.Iterable,
    ):
        """Make request to data source using `POST` with given payload

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            query_params (dict): request payload

        Returns:
            Any: request response
        """
        results = self.fetch(session, "post", url, data=payload)
        return results

    def fetch(self, session: requests.Session, *args, **kwargs):
        """Handles connection to given url with passed parameters and processing of request response

        Args:
            session (requests.Session): session with data source

        Raises:
            aiohttp.ServerConnectionError: Raises if data source recieved too many requests in a given amount of time

        Returns:
            str|dict|bytes: request response
        """

        @retry(
            retry=self.retry_condition,
            wait=tenacity.wait.wait_random_exponential(),
            stop=tenacity.stop.stop_after_attempt(5),
            after=tenacity.after.after_log(self.logger, _util.logging.WARNING),
        )
        def fetch_wrapper():
            with session.request(*args, **kwargs) as response:
                if response.status_code == requests.codes.too_many:
                    retry_after = response.headers.get("retry-after", 0)
                    err_msg = f"Too many requests. Trying again after {retry_after} sec"
                    self.logger.error(err_msg)
                    time.sleep(int(retry_after))
                    response.raise_for_status()

                if response.status_code != requests.codes.ok:
                    self.logger.error(
                        f"URL: {response.url}\n Status code: {response.status_code}\n Response: {response.text}"
                    )
                    response.raise_for_status()

                if self.response_type.lower() == "json":
                    result = response.json()
                elif self.response_type.lower() == "xml":
                    result = response.text
                else:
                    result = response.content

            return result

        return fetch_wrapper()


class AsyncAPIParser(APIParser):
    """Parser for data-sources that provide data via API (REST or another)"""

    def __init__(self, source: str, response_type: str, **kwargs) -> None:
        """Asyncronous parser for data-sources that provide data via API (REST or another)

        Args:
            source (str): URL to API
            response_type (str): Type of response from API (bytes, JSON, XML, plain text, etc.)

        KwArgs:
            user (str): Username to be used for authorization
            password (str): Password to be used for authorization
        """
        super().__init__(source, response_type, **kwargs)
        if self.user or self.password:
            self.auth = aiohttp.BasicAuth(self.user, self.password, encoding="utf-8")
        else:
            self.auth = None

        self.retry_condition = kwargs.pop("retry_condition", API_retry_if_status_code())

    def parse(self, method, request_params: dict = None, *args, **kwargs):
        """Parse data from API

        Args:
            method (str): REST method (GET, POST)
            request_params (dict, optional): Request params to use in requests to API. Defaults to `None`.

        Raises:
            ValueError: Raised if `method` not supported

        Returns:
            Any: API response
        """
        if request_params:
            payload = request_params
        else:
            payload = [request_params]

        ioloop = asyncio.get_event_loop()
        recieved_data = ioloop.run_until_complete(
            asyncio.ensure_future(self.async_parse(method, payload, *args, **kwargs))
        )
        return recieved_data

    def parse_many(
        self,
        method: str,
        requests_params: typing.Iterable | typing.TextIO = None,
        use_params_product: bool = False,
        *args,
        **kwargs,
    ):
        """Parse data source using each set of request parameters and return total result

        Args:
            method (str): method used to retrieve data (`GET`, `POST`, etc.)
            requests_params (typing.Iterable | typing.TextIO, optional): Sets of request parameters to use. Defaults to `None`. Params format: `[(key,value1),(key,value2),...]`
            use_params_product (bool, optional): Check to use cartesian product of request parameters. Defaults to `False`.

        KwArgs:
            break_time (float): pause between requests to API. Defaults to `0`.
            batch_size (int): batch size of request parameters to be processed at a time. Defaults to `10_000`
            api_url (str): relative url to API. Defaults to `''`.
            headers (dict): headers to be used during interaction with API. Defaults to `{}`.

        Returns:
            list: list of request replies
        """
        self.break_time = kwargs.pop("break_time", 0)

        # TODO Описать поведение, если получаемый массив параметров дохуя большой
        if requests_params:
            requests_payloads = requests_params
        else:
            requests_payloads = [requests_params]

        batch_size = kwargs.pop("batch_size", PARSER_BATCH_SIZE)
        if not None in requests_payloads:
            payloads = self.prepare_payloads(
                requests_payloads,
                use_params_product=use_params_product,
                batch_size=batch_size,
            )
        else:
            payloads = [requests_payloads]

        ioloop = asyncio.get_event_loop()
        recieved_data = ioloop.run_until_complete(
            asyncio.ensure_future(
                self.async_parse_many(
                    method,
                    payloads,
                    *args,
                    **kwargs,
                )
            )
        )

        return recieved_data

    async def async_parse(self, method: str, payload: dict, *args, **kwargs):
        """Make request with given payload

        Args:
            method (str): method used to make request (`GET`, `POST`, etc.)
            payload (dict): parameters to add in request

        Raises:
            ValueError: Raised if passed method isn't supported

        Returns:
            list: request response
        """
        url = urllib.parse.urljoin(self.source, kwargs.pop("api_url", ""))

        async with aiohttp.ClientSession(
            headers=kwargs.pop("headers", None), trust_env=True, auth=self.auth
        ) as session:
            recieved_data = []

            if method.upper() == "GET":
                recieved_data = await self.get(session, url, payload)
            elif method.upper() == "POST":
                recieved_data = await self.post(session, url, payload)
            else:
                err_msg = f"Method {method.upper()} is not supported"
                raise ValueError(err_msg)

        return recieved_data

    async def async_parse_many(
        self,
        method: str,
        payloads: typing.Iterable,
        *args,
        **kwargs,
    ):
        """Make requests with given payloads

        Args:
            method (str): method used to make requests (`GET`, `POST`, etc.)
            payloads (typing.Iterable): payloads added to requests

        Raises:
            ValueError: Raised if passed method isn't supported

        Returns:
            list: list of requests responses
        """
        url = urllib.parse.urljoin(self.source, kwargs.pop("api_url", ""))

        async with aiohttp.ClientSession(
            headers=kwargs.pop("headers", None), trust_env=True, auth=self.auth
        ) as session:
            recieved_data = []

            if method.upper() == "GET":
                recieved_data = await self.get_many(session, url, payloads)
            elif method.upper() == "POST":
                recieved_data = await self.post_many(session, url, payloads)
            else:
                err_msg = f"Method {method.upper()} is not supported"
                raise ValueError(err_msg)

        return recieved_data

    async def get_many(
        self,
        session: aiohttp.ClientSession,
        url: str,
        requests_batches: typing.Iterable,
    ):
        """Make requests to data source using `GET` with given payloads

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            requests_batches (typing.Iterable): requests payloads

        Returns:
            list: list of recieved responses
        """
        recieved_data = []
        for batch in requests_batches:
            time.sleep(self.break_time)
            for query_params in batch:
                query = (
                    urllib.parse.urlencode(query_params, doseq=True)
                    if any(query_params)
                    else None
                )
                recieved_data.append(
                    await self.fetch(session, "get", url, params=query)
                )

        return recieved_data

    async def get(
        self,
        session: aiohttp.ClientSession,
        url: str,
        query_params: dict,
    ):
        """Make request to data source using `GET` with given query parameters

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            query_params (dict): parameters to add to query url

        Returns:
            Any: request response
        """
        query = (
            urllib.parse.urlencode(query_params, doseq=True)
            if any(query_params)
            else None
        )
        recieved_data = await self.fetch(session, "get", url, params=query)

        return recieved_data

    async def post_many(
        self,
        session: aiohttp.ClientSession,
        url: str,
        requests_batches: typing.Iterable,
    ):
        """Make requests to data source using `POST` with given payloads

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            requests_batches (typing.Iterable): requests payloads

        Returns:
            list: list of recieved responses
        """
        results = []
        for batch in requests_batches:
            time.sleep(self.break_time)
            for payload in batch:
                results.append(await self.fetch(session, "post", url, data=payload))
        return results

    async def post(
        self,
        session: aiohttp.ClientSession,
        url: str,
        payload: typing.Iterable,
    ):
        """Make request to data source using `POST` with given payload

        Args:
            session (aiohttp.ClientSession): session with data source
            url (str): data source address
            query_params (dict): request payload

        Returns:
            Any: request response
        """
        results = await self.fetch(session, "post", url, data=payload)
        return results

    async def fetch(self, session: aiohttp.ClientSession, *args, **kwargs):
        """Handles connection to given url with passed parameters and processing of request response

        Args:
            session (aiohttp.ClientSession): session with data source

        Raises:
            aiohttp.ServerConnectionError: Raises if data source recieved too many requests in a given amount of time

        Returns:
            str|dict: request response
        """

        @retry(
            retry=self.retry_condition,
            wait=tenacity.wait.wait_random_exponential(),
            stop=tenacity.stop.stop_after_attempt(5),
            after=tenacity.after.after_log(self.logger, _util.logging.WARNING),
        )
        async def fetch_wrapper():
            async with session.request(*args, **kwargs) as response:
                if response.status == 429:
                    retry_after = response.headers.get("retry-after", 0)
                    err_msg = f"Too many requests. Trying again after {retry_after} sec"
                    self.logger.error(err_msg)
                    time.sleep(int(retry_after))
                    response.raise_for_status()

                if response.status != 200:
                    self.logger.error(
                        f"URL: {response.url}\n Status code: {response.status}\n Response: {response.text()}"
                    )
                    response.raise_for_status()

                if self.response_type.lower() == "json":
                    result = await response.json()
                elif self.response_type.lower() == "xml":
                    result = await response.text()
                else:
                    result = await response.read()

            return result

        return await fetch_wrapper()


class API_retry_if_status_code(tenacity.retry_base):
    def __init__(self, *status_codes: tuple[requests.structures.LookupDict]) -> None:
        super().__init__()
        self.status_codes = status_codes if status_codes else ()

    def __call__(self, retry_state: tenacity.RetryCallState) -> bool:
        exception = retry_state.outcome.exception()
        if issubclass(exception.__class__, Exception):
            if_retry = int(exception.response.status_code) in self.status_codes
        else:
            if_retry = False
        return if_retry


class API_retry_if_not_status_code(tenacity.retry_base):
    def __init__(self, *status_codes: tuple[requests.structures.LookupDict]) -> None:
        super().__init__()
        self.status_codes = status_codes if status_codes else ()

    def __call__(self, retry_state: tenacity.RetryCallState) -> bool:
        exception = retry_state.outcome.exception()
        if issubclass(exception.__class__, Exception):
            if_retry = int(exception.response.status_code) not in self.status_codes
        else:
            if_retry = False
        return if_retry
