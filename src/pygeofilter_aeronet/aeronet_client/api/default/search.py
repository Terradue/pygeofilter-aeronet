from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.search_avg import SearchAVG
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    year: int,
    month: int,
    day: int,
    avg: SearchAVG,
    aod10: int | Unset = UNSET,
    aod15: int | Unset = UNSET,
    aod20: int | Unset = UNSET,
    sda10: int | Unset = UNSET,
    sda15: int | Unset = UNSET,
    sda20: int | Unset = UNSET,
    tot10: int | Unset = UNSET,
    tot15: int | Unset = UNSET,
    tot20: int | Unset = UNSET,
    year2: int | Unset = UNSET,
    month2: int | Unset = UNSET,
    day2: int | Unset = UNSET,
    hour: int | Unset = UNSET,
    hour2: int | Unset = UNSET,
    site: str | Unset = UNSET,
    lat1: float | Unset = UNSET,
    lon1: float | Unset = UNSET,
    lat2: float | Unset = UNSET,
    lon2: float | Unset = UNSET,
    lunar_merge: int | Unset = UNSET,
    if_no_html: int | Unset = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["year"] = year

    params["month"] = month

    params["day"] = day

    json_avg = avg.value
    params["AVG"] = json_avg

    params["AOD10"] = aod10

    params["AOD15"] = aod15

    params["AOD20"] = aod20

    params["SDA10"] = sda10

    params["SDA15"] = sda15

    params["SDA20"] = sda20

    params["TOT10"] = tot10

    params["TOT15"] = tot15

    params["TOT20"] = tot20

    params["year2"] = year2

    params["month2"] = month2

    params["day2"] = day2

    params["hour"] = hour

    params["hour2"] = hour2

    params["site"] = site

    params["lat1"] = lat1

    params["lon1"] = lon1

    params["lat2"] = lat2

    params["lon2"] = lon2

    params["lunar_merge"] = lunar_merge

    params["if_no_html"] = if_no_html

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/cgi-bin/print_web_data_v3",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> str | None:
    if response.status_code == 200:
        response_200 = response.text
        return response_200

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[str]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    year: int,
    month: int,
    day: int,
    avg: SearchAVG,
    aod10: int | Unset = UNSET,
    aod15: int | Unset = UNSET,
    aod20: int | Unset = UNSET,
    sda10: int | Unset = UNSET,
    sda15: int | Unset = UNSET,
    sda20: int | Unset = UNSET,
    tot10: int | Unset = UNSET,
    tot15: int | Unset = UNSET,
    tot20: int | Unset = UNSET,
    year2: int | Unset = UNSET,
    month2: int | Unset = UNSET,
    day2: int | Unset = UNSET,
    hour: int | Unset = UNSET,
    hour2: int | Unset = UNSET,
    site: str | Unset = UNSET,
    lat1: float | Unset = UNSET,
    lon1: float | Unset = UNSET,
    lat2: float | Unset = UNSET,
    lon2: float | Unset = UNSET,
    lunar_merge: int | Unset = UNSET,
    if_no_html: int | Unset = UNSET,
) -> Response[str]:
    """Filters AERONET Version 3 products via web data service

    Args:
        year (int):
        month (int):
        day (int):
        avg (SearchAVG):
        aod10 (int | Unset):
        aod15 (int | Unset):
        aod20 (int | Unset):
        sda10 (int | Unset):
        sda15 (int | Unset):
        sda20 (int | Unset):
        tot10 (int | Unset):
        tot15 (int | Unset):
        tot20 (int | Unset):
        year2 (int | Unset):
        month2 (int | Unset):
        day2 (int | Unset):
        hour (int | Unset):
        hour2 (int | Unset):
        site (str | Unset):
        lat1 (float | Unset):
        lon1 (float | Unset):
        lat2 (float | Unset):
        lon2 (float | Unset):
        lunar_merge (int | Unset):
        if_no_html (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[str]
    """

    kwargs = _get_kwargs(
        year=year,
        month=month,
        day=day,
        avg=avg,
        aod10=aod10,
        aod15=aod15,
        aod20=aod20,
        sda10=sda10,
        sda15=sda15,
        sda20=sda20,
        tot10=tot10,
        tot15=tot15,
        tot20=tot20,
        year2=year2,
        month2=month2,
        day2=day2,
        hour=hour,
        hour2=hour2,
        site=site,
        lat1=lat1,
        lon1=lon1,
        lat2=lat2,
        lon2=lon2,
        lunar_merge=lunar_merge,
        if_no_html=if_no_html,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    year: int,
    month: int,
    day: int,
    avg: SearchAVG,
    aod10: int | Unset = UNSET,
    aod15: int | Unset = UNSET,
    aod20: int | Unset = UNSET,
    sda10: int | Unset = UNSET,
    sda15: int | Unset = UNSET,
    sda20: int | Unset = UNSET,
    tot10: int | Unset = UNSET,
    tot15: int | Unset = UNSET,
    tot20: int | Unset = UNSET,
    year2: int | Unset = UNSET,
    month2: int | Unset = UNSET,
    day2: int | Unset = UNSET,
    hour: int | Unset = UNSET,
    hour2: int | Unset = UNSET,
    site: str | Unset = UNSET,
    lat1: float | Unset = UNSET,
    lon1: float | Unset = UNSET,
    lat2: float | Unset = UNSET,
    lon2: float | Unset = UNSET,
    lunar_merge: int | Unset = UNSET,
    if_no_html: int | Unset = UNSET,
) -> str | None:
    """Filters AERONET Version 3 products via web data service

    Args:
        year (int):
        month (int):
        day (int):
        avg (SearchAVG):
        aod10 (int | Unset):
        aod15 (int | Unset):
        aod20 (int | Unset):
        sda10 (int | Unset):
        sda15 (int | Unset):
        sda20 (int | Unset):
        tot10 (int | Unset):
        tot15 (int | Unset):
        tot20 (int | Unset):
        year2 (int | Unset):
        month2 (int | Unset):
        day2 (int | Unset):
        hour (int | Unset):
        hour2 (int | Unset):
        site (str | Unset):
        lat1 (float | Unset):
        lon1 (float | Unset):
        lat2 (float | Unset):
        lon2 (float | Unset):
        lunar_merge (int | Unset):
        if_no_html (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        str
    """

    return sync_detailed(
        client=client,
        year=year,
        month=month,
        day=day,
        avg=avg,
        aod10=aod10,
        aod15=aod15,
        aod20=aod20,
        sda10=sda10,
        sda15=sda15,
        sda20=sda20,
        tot10=tot10,
        tot15=tot15,
        tot20=tot20,
        year2=year2,
        month2=month2,
        day2=day2,
        hour=hour,
        hour2=hour2,
        site=site,
        lat1=lat1,
        lon1=lon1,
        lat2=lat2,
        lon2=lon2,
        lunar_merge=lunar_merge,
        if_no_html=if_no_html,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    year: int,
    month: int,
    day: int,
    avg: SearchAVG,
    aod10: int | Unset = UNSET,
    aod15: int | Unset = UNSET,
    aod20: int | Unset = UNSET,
    sda10: int | Unset = UNSET,
    sda15: int | Unset = UNSET,
    sda20: int | Unset = UNSET,
    tot10: int | Unset = UNSET,
    tot15: int | Unset = UNSET,
    tot20: int | Unset = UNSET,
    year2: int | Unset = UNSET,
    month2: int | Unset = UNSET,
    day2: int | Unset = UNSET,
    hour: int | Unset = UNSET,
    hour2: int | Unset = UNSET,
    site: str | Unset = UNSET,
    lat1: float | Unset = UNSET,
    lon1: float | Unset = UNSET,
    lat2: float | Unset = UNSET,
    lon2: float | Unset = UNSET,
    lunar_merge: int | Unset = UNSET,
    if_no_html: int | Unset = UNSET,
) -> Response[str]:
    """Filters AERONET Version 3 products via web data service

    Args:
        year (int):
        month (int):
        day (int):
        avg (SearchAVG):
        aod10 (int | Unset):
        aod15 (int | Unset):
        aod20 (int | Unset):
        sda10 (int | Unset):
        sda15 (int | Unset):
        sda20 (int | Unset):
        tot10 (int | Unset):
        tot15 (int | Unset):
        tot20 (int | Unset):
        year2 (int | Unset):
        month2 (int | Unset):
        day2 (int | Unset):
        hour (int | Unset):
        hour2 (int | Unset):
        site (str | Unset):
        lat1 (float | Unset):
        lon1 (float | Unset):
        lat2 (float | Unset):
        lon2 (float | Unset):
        lunar_merge (int | Unset):
        if_no_html (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[str]
    """

    kwargs = _get_kwargs(
        year=year,
        month=month,
        day=day,
        avg=avg,
        aod10=aod10,
        aod15=aod15,
        aod20=aod20,
        sda10=sda10,
        sda15=sda15,
        sda20=sda20,
        tot10=tot10,
        tot15=tot15,
        tot20=tot20,
        year2=year2,
        month2=month2,
        day2=day2,
        hour=hour,
        hour2=hour2,
        site=site,
        lat1=lat1,
        lon1=lon1,
        lat2=lat2,
        lon2=lon2,
        lunar_merge=lunar_merge,
        if_no_html=if_no_html,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    year: int,
    month: int,
    day: int,
    avg: SearchAVG,
    aod10: int | Unset = UNSET,
    aod15: int | Unset = UNSET,
    aod20: int | Unset = UNSET,
    sda10: int | Unset = UNSET,
    sda15: int | Unset = UNSET,
    sda20: int | Unset = UNSET,
    tot10: int | Unset = UNSET,
    tot15: int | Unset = UNSET,
    tot20: int | Unset = UNSET,
    year2: int | Unset = UNSET,
    month2: int | Unset = UNSET,
    day2: int | Unset = UNSET,
    hour: int | Unset = UNSET,
    hour2: int | Unset = UNSET,
    site: str | Unset = UNSET,
    lat1: float | Unset = UNSET,
    lon1: float | Unset = UNSET,
    lat2: float | Unset = UNSET,
    lon2: float | Unset = UNSET,
    lunar_merge: int | Unset = UNSET,
    if_no_html: int | Unset = UNSET,
) -> str | None:
    """Filters AERONET Version 3 products via web data service

    Args:
        year (int):
        month (int):
        day (int):
        avg (SearchAVG):
        aod10 (int | Unset):
        aod15 (int | Unset):
        aod20 (int | Unset):
        sda10 (int | Unset):
        sda15 (int | Unset):
        sda20 (int | Unset):
        tot10 (int | Unset):
        tot15 (int | Unset):
        tot20 (int | Unset):
        year2 (int | Unset):
        month2 (int | Unset):
        day2 (int | Unset):
        hour (int | Unset):
        hour2 (int | Unset):
        site (str | Unset):
        lat1 (float | Unset):
        lon1 (float | Unset):
        lat2 (float | Unset):
        lon2 (float | Unset):
        lunar_merge (int | Unset):
        if_no_html (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        str
    """

    return (
        await asyncio_detailed(
            client=client,
            year=year,
            month=month,
            day=day,
            avg=avg,
            aod10=aod10,
            aod15=aod15,
            aod20=aod20,
            sda10=sda10,
            sda15=sda15,
            sda20=sda20,
            tot10=tot10,
            tot15=tot15,
            tot20=tot20,
            year2=year2,
            month2=month2,
            day2=day2,
            hour=hour,
            hour2=hour2,
            site=site,
            lat1=lat1,
            lon1=lon1,
            lat2=lat2,
            lon2=lon2,
            lunar_merge=lunar_merge,
            if_no_html=if_no_html,
        )
    ).parsed
