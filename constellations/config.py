import re
from pathlib import Path


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
        self._ip = address[0]
        self._port = int(address[1])
        self._chunks = chunks

    @property
    def ip(self) -> str:
        return self._ip

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

        succeed = re.match(r'^[\w\d.]*:[\d]*$', peer)
        if not succeed:
            raise ValueError(
                f'Peer\'s address must be defined as ip:port. Got {peer}'
            )

        return FileConfigOut(str(file), peer, hash_sum, chunks)

    @staticmethod
    def create_in(path: str, hash_sum: str) -> FileConfigIn:
        file = Path(path)
        if not file.is_file():
            raise FileNotFoundError('File %s not found' % path)

        return FileConfigIn(str(file), hash_sum)
