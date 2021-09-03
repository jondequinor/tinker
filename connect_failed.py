import asyncio
from asyncio.tasks import wait
import time
from http import HTTPStatus

import aiohttp
import websockets


async def hello(websocket, path):
    name = await websocket.recv()
    print(f"< {name}")

    greeting = f"Hello {name}!"

    await websocket.send(greeting)
    print(f"> {greeting}")


async def attempt_connection(
    url: str,
    connection_timeout: float = 2,
) -> None:
    timeout = aiohttp.ClientTimeout(connect=connection_timeout)
    async with aiohttp.ClientSession() as session:
        async with session.request(
            method="get",
            url=url,
            timeout=timeout,
        ) as resp:
            resp.raise_for_status()


async def wait_for_connection(
    base_url: str,
    path: str = "/healthcheck",
    timeout: float = 60,
    connection_timeout: float = 2,
) -> None:
    healthcheck_url = base_url + path
    start = time.time()
    sleep_time = 0.2
    sleep_time_max = 5.0
    while time.time() - start < timeout:
        try:
            await attempt_connection(
                url=healthcheck_url,
                connection_timeout=connection_timeout,
            )
            return
        except aiohttp.ClientError:
            sleep_time = min(sleep_time_max, sleep_time * 2)
            remaining_time = max(0, timeout - (time.time() - start) + 0.1)
            await asyncio.sleep(min(sleep_time, remaining_time))

    # We have timed out, but we make one last attempt to ensure that
    # we have tried to connect at both ends of the time window
    await attempt_connection(
        url=healthcheck_url,
        connection_timeout=connection_timeout,
    )


async def process_request(path, request_headers):
    if path == "/healthcheck":
        return HTTPStatus.OK, {}, b""


async def client():
    await wait_for_connection("ws://localhost:8765")
    async with websockets.connect("ws://localhost:8765") as websocket:
        name = "Friend"
        await websocket.send(name)
        print(f"> {name}")

        greeting = await websocket.recv()
        print(f"< {greeting}")


async def main():
    server = websockets.serve(
        hello,
        "localhost",
        8765,
        process_request=process_request,
    )
    await asyncio.wait([server, client()])

if __name__ == "__main__":
    asyncio.run(main())
