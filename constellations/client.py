import asyncio
import logging
import shutil
import socket
from pathlib import Path
from typing import Tuple

from constellations.config import FileConfigOut

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, file_config_out: FileConfigOut, chunk_size: int):
        self._folder_suffix = '_chunks'
        self._file_config_out = file_config_out
        self._chunk_size = chunk_size

    async def consume(self) -> None:
        reader, writer = await self.connect_to_client()

        try:
            logger.debug(f'{self._file_config_out.hash_sum}: Sending hash sum of file.')
            writer.write(self._file_config_out.hash_sum.encode('utf8'))
            await writer.drain()

            response = await asyncio.wait_for(reader.read(self._chunk_size), 3.0)
            logger.debug(f'{self._file_config_out.hash_sum}: Got response: {response}.')

            if response.decode('utf8') != 'ok':
                logger.info(f'{self._file_config_out.hash_sum}: Can\'t get the file from remote server.')
                raise Exception(
                    'Error while fetching the file %s' % self._file_config_out.__dict__
                )

            folder_with_chunks = self.create_folder_with_chunks()
            await self.process_data_from_peer(reader, writer, folder_with_chunks)
            self.merge_chunks_to_file(folder_with_chunks)
        finally:
            writer.close()

    async def connect_to_client(
        self,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    self._file_config_out.ip, self._file_config_out.port
                )
                return reader, writer
            except OSError as e:
                logger.info(
                    '%s: %s:%d', e, self._file_config_out.ip, self._file_config_out.port
                )
                await asyncio.sleep(5)

    def create_folder_with_chunks(self) -> Path:
        result_file = Path(self._file_config_out.path)
        folder_with_chunks = result_file.parent / (
            result_file.stem + self._folder_suffix
        )
        folder_with_chunks.mkdir(parents=True, exist_ok=True)

        return folder_with_chunks

    async def process_data_from_peer(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        folder_with_chunks: Path,
    ) -> None:
        for i in range(0, self._file_config_out.chunks):
            chunk_file = folder_with_chunks / str(i)
            if not chunk_file.is_file():
                logger.debug(f'Sending a chunk id: {i}')
                writer.write(str(i).encode('utf8'))
                await writer.drain()

                response = await asyncio.wait_for(reader.read(self._chunk_size), 3.0)

                logger.debug(f'Write chunk to a file')
                with chunk_file.open('w') as opened_file:
                    opened_file.write(response.decode('utf8'))

    def merge_chunks_to_file(self, folder_with_chunks: Path) -> None:
        result_file = Path(self._file_config_out.path)
        with result_file.open('w') as opened_file:
            for chunk_file in sorted(folder_with_chunks.iterdir()):
                with chunk_file.open('r') as opened_chunk_file:
                    opened_file.write(opened_chunk_file.read())

        shutil.rmtree(folder_with_chunks)
