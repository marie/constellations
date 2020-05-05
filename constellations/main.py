import asyncio
import re
import shutil
from pathlib import Path
from typing import List, Optional, Tuple

import yaml


class FileConfig:
    def __init__(self, path: str, hash_sum: str):
        self._path = path
        self._hash_sum = hash_sum

    @property
    def path(self) -> str:
        return self._path

    @property
    def hash_sum(self) -> str:
        return self._hash_sum


class FileConfigOut(FileConfig):
    def __init__(
        self, path: str, peer: str, hash_sum: str, chunks: int,
    ):
        super().__init__(path, hash_sum)
        address = peer.split(':')
        self._peer = address[0]
        self._port = int(address[1])
        self._chunks = chunks

    @property
    def peer(self) -> str:
        return self._peer

    @property
    def port(self) -> int:
        return self._port

    @property
    def chunks(self) -> int:
        return self._chunks


class FileConfigIn(FileConfig):
    pass


class FileConfigFactory:
    @staticmethod
    def create_out(path: str, peer: str, hash_sum: str, chunks: int) -> FileConfigOut:
        file = Path(path)
        if not file.is_file():
            file.touch()

        succeed = re.match(r'^[\w]*:[\d]*$', peer)
        if not succeed:
            raise ValueError(
                'Peer\'s address must be defined as ip:port. Got %s' % peer
            )

        return FileConfigOut(str(file), peer, hash_sum, chunks)

    @staticmethod
    def create_in(path: str, hash_sum: str) -> FileConfigIn:
        file = Path(path)
        if not file.is_file():
            raise FileNotFoundError('File %s not found' % path)

        return FileConfigIn(str(file), hash_sum)


class Client:
    def __init__(self, file_config_out: FileConfigOut, chunk_size: int):
        self._folder_suffix = '_chunks'
        self._file_config_out = file_config_out
        self._chunk_size = chunk_size

    async def consume(self) -> None:
        reader, writer = await self.connect_to_client()

        try:
            writer.write(self._file_config_out.hash_sum.encode('utf8'))
            await writer.drain()

            response = await asyncio.wait_for(reader.read(self._chunk_size), 3.0)
            if response.decode('utf8') != 'ok':
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
                    self._file_config_out.peer, self._file_config_out.port
                )
                return reader, writer
            except ConnectionRefusedError:
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
                writer.write(str(i).encode('utf8'))
                await writer.drain()
                response = await asyncio.wait_for(reader.read(self._chunk_size), 3.0)

                with chunk_file.open('w') as opened_file:
                    opened_file.write(response.decode('utf8'))

    def merge_chunks_to_file(self, folder_with_chunks: Path) -> None:
        result_file = Path(self._file_config_out.path)
        with result_file.open('w') as opened_file:
            for chunk_file in sorted(folder_with_chunks.iterdir()):
                with chunk_file.open('r') as opened_chunk_file:
                    opened_file.write(opened_chunk_file.read())

        shutil.rmtree(folder_with_chunks)


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
            raise FileNotFoundError('File with hash_sum %s not found' % hash_sum)

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

                if not chunk_id:
                    writer.close()
                    return

                opened_file.seek(int(chunk_id) * self._chunk_size)
                chunk = opened_file.read(self._chunk_size)
                writer.write(chunk.encode('utf8'))
                await writer.drain()


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
