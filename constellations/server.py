import asyncio
import logging
from pathlib import Path
from typing import List, Optional

from constellations.config import FileConfigIn

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, files: List[FileConfigIn], chunk_size: int):
        self._files = files
        self._chunk_size = chunk_size

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        hash_sum = (await reader.read(65)).decode('utf8')
        file_information = self.get_file(hash_sum)

        if not file_information:
            writer.write('error'.encode('utf8'))
            writer.close()
            logger.info(f'File with hash_sum {hash_sum} not found')
            raise FileNotFoundError(f'File with hash_sum {hash_sum} not found')

        writer.write('ok'.encode('utf8'))
        await writer.drain()

        await self.send_chunks(file_information, reader, writer)

    def get_file(self, hash_sum: str) -> Optional[FileConfigIn]:
        return next((item for item in self._files if item.hash_sum == hash_sum), None)

    async def send_chunks(
        self,
        file_information: FileConfigIn,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        file = Path(file_information.path)
        with file.open() as opened_file:
            while True:
                chunk_id = (await reader.read(32)).decode('utf8')
                logger.debug(f'Got chunk id: {chunk_id}')

                if not chunk_id:
                    writer.close()
                    return

                opened_file.seek(int(chunk_id) * self._chunk_size)
                chunk = opened_file.read(self._chunk_size)

                logger.debug(f'Sending a chunk')
                writer.write(chunk.encode('utf8'))
                await writer.drain()
