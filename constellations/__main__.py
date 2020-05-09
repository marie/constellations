import logging

from constellations.main import main
from environs import Env

if __name__ == '__main__':
    env = Env()
    env.read_env()

    logging.basicConfig(
        filename=env('LOG_FILE'),
        level=env('LOG_LEVEL', 'INFO').upper(),
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
    )

    main(
        env('LISTEN_HOST'),
        env.int('LISTEN_PORT'),
        env('DOWNLOAD_FILES'),
        env('SHARE_FILES'),
        env.int('CHUNK_SIZE'),
    )
