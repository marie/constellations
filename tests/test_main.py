import asyncio
import hashlib
import tempfile
from pathlib import Path
from unittest.mock import patch

import asynctest
import pytest
from constellations.client import Client
from constellations.config import FileConfigFactory
from constellations.main import main
from constellations.server import Server


class TestFileConfigFactory:
    def test_create_out(self):
        data = {
            'path': '/test_files/book',
            'peer': 'peer_address:8080',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
            'chunks': 1,
        }

        with patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            file_config = FileConfigFactory.create_out(**data)
            assert file_config.path == data['path']
            assert file_config.ip == 'peer_address'
            assert file_config.port == 8080
            assert file_config.hash_sum == data['hash_sum']
            assert file_config.chunks == data['chunks']

    def test_create_out_rises_exception(self):
        data = {
            'path': '/test_files/book',
            'peer': 'wrong_address',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
            'chunks': 1,
        }
        with pytest.raises(ValueError), patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            FileConfigFactory.create_out(**data)

    def test_create_in(self):
        data = {
            'path': '/test_files/book',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
        }

        with patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            file_config = FileConfigFactory.create_in(**data)
            assert file_config.path == data['path']
            assert file_config.hash_sum == data['hash_sum']

    def test_create_in_raises_exception(self):
        data = {
            'path': '/test_files/book',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
        }

        with pytest.raises(FileNotFoundError):
            FileConfigFactory.create_in(**data)


class TestClient:
    @pytest.mark.asyncio
    async def test_consume(self):
        data = {
            'path': '/test_files/book',
            'peer': 'peer_address:8080',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
            'chunks': 1,
        }

        with patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            file_config = FileConfigFactory.create_out(**data)

        with patch(
            'constellations.main.Client.connect_to_client'
        ) as connect_to_client_mock, patch(
            'asyncio.StreamReader', new=asynctest.CoroutineMock()
        ) as reader_mock, patch(
            'asyncio.StreamReader.read', new=asynctest.CoroutineMock()
        ), patch(
            'asyncio.StreamWriter', new=asynctest.CoroutineMock()
        ) as writer_mock, patch(
            'asyncio.StreamWriter.drain', new=asynctest.CoroutineMock()
        ), pytest.raises(
            Exception, match=r'Error while fetching the file.*'
        ):
            connect_to_client_mock.return_value = (reader_mock, writer_mock)
            client = Client(file_config, 2)
            await client.consume()


class TestServer:
    @pytest.mark.asyncio
    async def test_handle_client(self):
        data = {
            'path': 'test_files/book',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
        }

        with patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            file_config = FileConfigFactory.create_in(**data)
        with patch(
            'asyncio.StreamReader', new=asynctest.CoroutineMock()
        ) as reader_mock, patch(
            'asyncio.StreamReader.read', new=asynctest.CoroutineMock()
        ), patch(
            'asyncio.StreamWriter', new=asynctest.CoroutineMock()
        ) as writer_mock, pytest.raises(
            Exception, match=r'File with hash_sum .* not found'
        ):
            client = Server([file_config], 2)
            await client.handle_client(reader_mock, writer_mock)

    def test_get_file_exists(self):
        data = {
            'path': 'test_files/book',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
        }

        with patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            file_config = FileConfigFactory.create_in(**data)

        server = Server([file_config], 2)
        result = server.get_file('c6e6f16b8a077ef5fbc8d59d0b931b9')

        assert result == file_config

    def test_get_file_not_exists(self):
        data = {
            'path': 'test_files/book',
            'hash_sum': 'c6e6f16b8a077ef5fbc8d59d0b931b9',
        }

        with patch('pathlib.Path.is_file') as is_file_mock:
            is_file_mock.return_value = True
            file_config = FileConfigFactory.create_in(**data)

        server = Server([file_config], 2)
        result = server.get_file('wrong_hash')

        assert result is None


@pytest.mark.asyncio
async def test_peer_to_peer():
    chunk_size = 2
    temp_dir = Path(tempfile.gettempdir())

    book_on_server = Path(temp_dir / 'awesome_book_on_server')
    with book_on_server.open('w') as open_file:
        open_file.write('Hello, world')

    server_file = {
        'path': temp_dir / 'awesome_book_on_server',
        'hash_sum': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    }

    file_config_in = FileConfigFactory.create_in(**server_file)
    server = Server([file_config_in], chunk_size)
    asyncio.ensure_future(asyncio.start_server(server.handle_client, '0.0.0.0', 8181))

    client_file = {
        'path': temp_dir / 'awesome_book_on_client',
        'peer': 'localhost:8181',
        'hash_sum': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        'chunks': 6,
    }
    file_config_out = FileConfigFactory.create_out(**client_file)
    client = Client(file_config_out, chunk_size)
    await client.consume()

    book_on_client = Path(file_config_out.path)
    assert (
        hashlib.sha256(book_on_client.read_bytes()).hexdigest()
        == hashlib.sha256(book_on_server.read_bytes()).hexdigest()
    )

    book_on_client.unlink()
    book_on_server.unlink()


def test_main():
    with patch('asyncio.unix_events._UnixSelectorEventLoop.run_forever'), patch(
        'asyncio.start_server'
    ) as start_server_mock, patch('pathlib.Path.open'), patch(
        'constellations.server.Server.handle_client'
    ) as handle_client_mock, patch(
        'yaml.safe_load'
    ) as safe_load_mock:
        main('localhost', 8080, '/download_files.yml', '/share_files.yml', 2)
        safe_load_mock.assert_called()
        start_server_mock.assert_called_with(handle_client_mock, 'localhost', 8080)
