from constellations.main import main
from environs import Env

if __name__ == '__main__':
    env = Env()
    env.read_env()

    main(
        env('LISTEN_HOST'),
        env.int('LISTEN_PORT'),
        env('DOWNLOAD_FILES'),
        env('SHARE_FILES'),
        env.int('CHUNK_SIZE'),
    )
