import asyncio
import re
import shutil
from pathlib import Path
from typing import List, Optional, Tuple

import yaml

from constellations.client import Client
from constellations.config import FileConfigFactory
from constellations.server import Server


def main(
    listen_host: str,
    listen_port: int,
    download_files_yaml: str,
    share_files_yaml: str,
    chunk_size: int,
) -> None:
    with Path(download_files_yaml).open('r') as files_collection:
        download_files = yaml.safe_load(files_collection) or []

    tasks = []
    for file in download_files:
        client = Client(FileConfigFactory.create_out(**file), chunk_size)
        tasks.append(client.consume())

    asyncio.gather(*tasks)

    with Path(share_files_yaml).open('r') as files_collection:
        share_files = yaml.safe_load(files_collection) or []

    files = []
    for file in share_files:
        files.append(FileConfigFactory.create_in(**file))

    server = Server(files, chunk_size)
    loop = asyncio.get_event_loop()
    loop.create_task(
        asyncio.start_server(server.handle_client, listen_host, listen_port)
    )
    loop.run_forever()
