import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, List, Union

import yaml
from constellations.client import Client
from constellations.config import FileConfigFactory
from constellations.server import Server
from yaml.scanner import ScannerError

logger = logging.getLogger(__name__)


def parse_yaml(yaml_path: str) -> Union[Any, List[Any]]:
    with Path(yaml_path).open('r') as files_collection:
        logger.debug('Load collection from %s ', yaml_path)
        return yaml.safe_load(files_collection) or []


def main(
    listen_host: str,
    listen_port: int,
    download_files_yaml: str,
    share_files_yaml: str,
    chunk_size: int,
) -> None:
    try:
        download_files = parse_yaml(download_files_yaml)
        share_files = parse_yaml(share_files_yaml)
    except FileNotFoundError as e:
        print('File with collection not found. For more information read the log file.')
        logger.fatal('%s', e)
        sys.exit()
    except ScannerError as e:
        print('Yaml file is not valid. For more information read the log file.')
        logger.fatal('%s', e)
        sys.exit()

    tasks = []
    for file in download_files:
        client = Client(FileConfigFactory.create_out(**file), chunk_size)
        tasks.append(client.consume())

    asyncio.gather(*tasks)

    files = []
    for file in share_files:
        files.append(FileConfigFactory.create_in(**file))

    server = Server(files, chunk_size)
    loop = asyncio.get_event_loop()
    loop.create_task(
        asyncio.start_server(server.handle_client, listen_host, listen_port)
    )

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        sys.exit()
