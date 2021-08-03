import sys
import time
import socket
import shutil
import logging
import pathlib
import colorama

import db
import utils
import models
import configs

from urllib.request import urlopen
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound

CONNECTIVITY_CHECK_URL = 'https://www.baidu.com'
CONFIG_PATH = 'configs'


def panic(message: str):
    logging.critical(message)
    sys.exit(-1)


def check_connectivity():
    tries = 0
    while True:
        try:
            urlopen(CONNECTIVITY_CHECK_URL, timeout=30)
        except Exception as e:
            if tries < RETRIES:
                logging.error(f'{colorama.Fore.LIGHTRED_EX}'
                              f'Connectivity check failed: {e}'
                              f'{colorama.Fore.RESET}')
                logging.info(f'Retry after {RETRY_INTERVAL} seconds ({RETRIES - tries} left)).')
                time.sleep(RETRY_INTERVAL)
                tries += 1
            else:
                panic(f'{colorama.Fore.LIGHTRED_EX}'
                      f'Connectivity check failed after {RETRIES} retries.'
                      f'{colorama.Fore.RESET}')
            continue
        break


def find_device_by_name(session: Session, name: str) -> models.Device:
    try:
        device = session.query(models.Device).filter_by(device_name=name).one()
    except NoResultFound:
        device = models.Device()
        device.device_name = name
        session.add(device)
        session.commit()
    return device


def find_data_by_uri(session: Session, uri: str) -> models.Data:
    return session \
        .query(models.Data) \
        .filter_by(uri=uri) \
        .first()


def main():
    db_engine = db.db_connect(DB_CONF)
    device_path = pathlib.Path(DEVICE_PATH)
    copy_source = pathlib.Path(COPY_SOURCE)
    copy_dest = pathlib.Path(COPY_DEST)

    try:
        check_connectivity()
    except KeyboardInterrupt:
        logging.info(f'Bye.')
        return

    if COPY_ENABLED:
        logging.info(f'Scanning source folder: '
                     f'{colorama.Fore.LIGHTCYAN_EX}'
                     f'{{path={COPY_SOURCE}}}'
                     f'{colorama.Fore.RESET}'
                     f'.')
        file_list = list(copy_source.rglob('*.warc.wet.json'))
    else:
        logging.info(f'Scanning device: '
                     f'{colorama.Fore.LIGHTCYAN_EX}'
                     f'{{path={device_path}}}'
                     f'{colorama.Fore.RESET}'
                     f'.')
        file_list = list(device_path.rglob('*.warc.wet.json'))

    session = Session(bind=db_engine)
    progbar = utils.ProgBar(target=len(file_list))

    for file in file_list:
        size = file.stat().st_size
        if size < 1000:
            logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                            f'Ignoring corrupted file: '
                            f'{colorama.Fore.RESET}'
                            f'{colorama.Fore.LIGHTCYAN_EX}'
                            f'{{{file}}}'
                            f'{colorama.Fore.RESET}'
                            f'{colorama.Fore.LIGHTYELLOW_EX}'
                            f'.'
                            f'{colorama.Fore.RESET}')
        else:
            if COPY_ENABLED:
                segments = str(file.relative_to(copy_source))
                full_path = copy_dest.joinpath(segments)
                full_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(file, full_path)
                full_path = str(full_path.relative_to(device_path).as_posix())
            else:
                full_path = str(file.relative_to(device_path).as_posix())
            archive, remain = full_path.split('/', maxsplit=1)
            out_path_idx = remain.find('crawl-data/')
            prefix = remain[:out_path_idx - 1]
            out_path = remain[out_path_idx:]
            uri = str(pathlib.Path(out_path).with_suffix('.gz').as_posix())

            data = find_data_by_uri(session, uri=uri)
            if data.storage is None:
                storage = models.Storage()
            else:
                storage = data.storage

            storage.device = find_device_by_name(session, name=DEVICE_NAME)
            storage.archive = archive
            storage.prefix = prefix
            storage.out_path = out_path
            storage.size = size

            data.storage = storage

            session.add(storage)
            session.add(data)

            session.commit()
        progbar.add(1)

    session.close()


if __name__ == '__main__':
    config = configs.config(CONFIG_PATH)
    DB_CONF = db.get_database_config(config)
    DEVICE_NAME = config.get('worker', 'device_name')
    RETRY_INTERVAL = config.getint('worker', 'retry_interval')
    RETRIES = config.getint('worker', 'retries')
    SOCKET_TIMEOUT = config.getint('worker', 'socket_timeout')
    DEVICE_PATH = config.get('worker', 'device_path')
    COPY_ENABLED = config.getboolean('copy', 'enabled')
    COPY_SOURCE = config.get('copy', 'source')
    COPY_DEST = config.get('copy', 'destination')

    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
