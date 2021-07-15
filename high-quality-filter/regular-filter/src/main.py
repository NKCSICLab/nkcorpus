import sys
import time
import pytz
import random
import socket
import logging
import pathlib
import datetime
import colorama
import json as pyjson
import simdjson as json

from typing import Sequence, Tuple
from urllib.request import urlopen
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy import text

import db
import utils
import models
import configs

CONNECTIVITY_CHECK_URL = 'https://www.baidu.com'
TIMEZONE = 'Asia/Shanghai'
CONFIG_PATH = 'configs'


def panic(message: str):
    logging.critical(message)
    sys.exit(-1)


def check_connectivity():
    tries = 0
    while True:
        try:
            urlopen(CONNECTIVITY_CHECK_URL, timeout=SOCKET_TIMEOUT)
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


def filter_data(data: pathlib.Path,
                filters: Sequence[models.Filter],
                filtered_clean_data: pathlib.Path,
                filtered_deleted_data: pathlib.Path) -> Tuple[int, int]:
    with \
            open(data, 'rb') as f_data, \
            open(filtered_clean_data, 'w') as f_clean, \
            open(filtered_deleted_data, 'w') as f_deleted:
        data = json.load(f_data)['data']
        clean_data, deleted_data = utils.filter_data(data, filters=filters)
        json.dump(clean_data, f_clean)
        json.dump(deleted_data, f_deleted)
    return filtered_clean_data.stat().st_size, filtered_deleted_data.stat().st_size


def find_device_by_name(session: Session, name: str) -> models.Device:
    try:
        device = session.query(models.Device).filter_by(device_name=name).one()
    except NoResultFound:
        device = models.Device()
        device.device_name = name
        session.add(device)
        session.commit()
    return device


def find_job_by_prefix_and_out_path(session: Session, prefix: str, out_path: str) -> models.Data:
    return session \
        .query(models.Data) \
        .join(models.Storage) \
        .filter(models.Storage.prefix == prefix,
                models.Storage.out_path == out_path) \
        .one()


def find_filters_by_id(session: Session, id_list: Sequence) -> Sequence[models.Filter]:
    logging.info(f'Fetching filters from database...')
    filters: Sequence[models.Filter] = session \
        .query(models.Filter) \
        .filter(models.Filter.id.in_(id_list)) \
        .order_by(text(f'field(id, {", ".join([str(id_) for id_ in id_list])})')) \
        .all()
    filters_fetched_str = ', '.join([str(filter_.id) for filter_ in filters])
    logging.info(f'Filters fetched: '
                 f'{colorama.Fore.LIGHTCYAN_EX}'
                 f'{{id=[{filters_fetched_str}]}}'
                 f'{colorama.Fore.RESET}'
                 f'.')
    return filters


def update_storage(session: Session, parameters: dict) -> models.Storage:
    try:
        storage: models.Storage = session \
            .query(models.Storage) \
            .filter_by(archive=parameters['archive'],
                       prefix=parameters['prefix'],
                       out_path=parameters['out_path']) \
            .one()
        storage.device = parameters['device']
        storage.size = parameters['size']
    except NoResultFound:
        storage: models.Storage = models.Storage(
            device=parameters['device'],
            archive=parameters['archive'],
            prefix=parameters['prefix'],
            out_path=parameters['out_path'],
            size=parameters['size']
        )
    return storage


def update_filtered(session: Session, parameters: dict) -> models.Filtered:
    try:
        filtered: models.Filtered = session \
            .query(models.Filtered) \
            .filter_by(data=parameters['data'],
                       filters=parameters['filters'],
                       storage=parameters['storage']) \
            .one()
        filtered.data = parameters['data']
        filtered.filters = parameters['filters']
        filtered.storage = parameters['storage']
    except NoResultFound:
        filtered: models.Filtered = models.Filtered(
            data=parameters['data'],
            filters=parameters['filters'],
            storage=parameters['storage']
        )
    return filtered


def main():
    db_engine = db.db_connect(DB_CONF)
    data_path = pathlib.Path(DATA_ROOT).joinpath(ARCHIVE, DATA_PREFIX)
    while True:
        try:
            check_connectivity()
        except KeyboardInterrupt:
            logging.info(f'Bye.')
            return

        logging.info('Fetching a new job...')
        session = Session(bind=db_engine)
        tries = 0
        out_path = None
        while True:
            try:
                logging.info('Scanning data folder...')
                data_list = list(data_path.rglob('*.warc.wet.json'))
                if len(data_list) == 0:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                out_path = str(random.choice(data_list).relative_to(data_path).as_posix())
                session.begin()
                job: models.Data = session \
                    .query(models.Data) \
                    .join(models.Storage) \
                    .with_for_update(of=models.Data, skip_locked=True) \
                    .filter(models.Storage.prefix == DATA_PREFIX,
                            models.Storage.out_path == out_path,
                            models.Data.filter_state == models.Data.FILTER_PENDING) \
                    .first()
                if job is None:
                    session.commit()
                    session.close()
                    logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'File: '
                                    f'{colorama.Fore.RESET}'
                                    f'{colorama.Fore.LIGHTCYAN_EX}'
                                    f'{{prefix={DATA_PREFIX}}}'
                                    f'{{out_path={out_path}}}'
                                    f'{colorama.Fore.RESET} '
                                    f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'is not in the database or is being processed by another worker.'
                                    f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds.')
                    time.sleep(RETRY_INTERVAL)
                    continue
                processed: models.Filtered = session \
                    .query(models.Filtered) \
                    .filter_by(id_data=job.id,
                               filters=filters_str) \
                    .first()
                if processed is not None:
                    session.commit()
                    session.close()
                    logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'File: '
                                    f'{colorama.Fore.RESET}'
                                    f'{colorama.Fore.LIGHTCYAN_EX}'
                                    f'{{prefix={DATA_PREFIX}}}'
                                    f'{{out_path={out_path}}}'
                                    f'{colorama.Fore.RESET} '
                                    f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'is already processed.'
                                    f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds.')
                    time.sleep(RETRY_INTERVAL)
                    continue
                job.filter_state = models.Data.FILTER_PROCESSING
                job.start_deal_time = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                session.add(job)
                session.commit()
                logging.info(f'New job fetched: '
                             f'{colorama.Fore.LIGHTCYAN_EX}'
                             f'{{id={job.id}, uri={job.uri}}}'
                             f'{colorama.Fore.RESET}'
                             f'.')
                session.close()
            except Exception as e:
                if tries < RETRIES:
                    session.rollback()
                    logging.error(f'{colorama.Fore.LIGHTRED_EX}'
                                  f'An error has occurred: {e}'
                                  f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds ({RETRIES - tries} left)).')
                    time.sleep(RETRY_INTERVAL)
                    tries += 1
                else:
                    panic(f'{colorama.Fore.LIGHTRED_EX}'
                          f'Failed to fetch a new job after {RETRIES} retries.'
                          f'{colorama.Fore.RESET}')
                continue
            break
        session = Session(bind=db_engine)
        try:
            tries = 0
            while True:
                try:
                    unprocessed_data = pathlib.Path(data_path).joinpath(out_path)
                    filtered_clean_data = pathlib \
                        .Path(DATA_ROOT) \
                        .joinpath(ARCHIVE, FILTERED_CLEAN_PREFIX, filters_str, out_path)
                    filtered_deleted_data = pathlib \
                        .Path(DATA_ROOT) \
                        .joinpath(ARCHIVE, FILTERED_DELETED_PREFIX, filters_str, out_path)
                    filtered_clean_data.parent.mkdir(parents=True, exist_ok=True)
                    filtered_deleted_data.parent.mkdir(parents=True, exist_ok=True)
                    filters = find_filters_by_id(session, FILTERS)
                    clean_size, deleted_size = filter_data(data=unprocessed_data,
                                                           filters=filters,
                                                           filtered_clean_data=filtered_clean_data,
                                                           filtered_deleted_data=filtered_deleted_data)
                    job = find_job_by_prefix_and_out_path(session=session, prefix=DATA_PREFIX, out_path=out_path)
                    device = find_device_by_name(session=session, name=DEVICE)
                    clean_storage = update_storage(session, parameters={
                        'device': device,
                        'archive': ARCHIVE,
                        'prefix': pathlib.Path(FILTERED_CLEAN_PREFIX).joinpath(filters_str).as_posix(),
                        'out_path': out_path,
                        'size': clean_size
                    })
                    clean_filtered = update_filtered(session, parameters={
                        'data': job,
                        'filters': filters_str,
                        'storage': clean_storage
                    })
                    deleted_storage = update_storage(session, parameters={
                        'device': device,
                        'archive': ARCHIVE,
                        'prefix': pathlib.Path(FILTERED_DELETED_PREFIX).joinpath(filters_str).as_posix(),
                        'out_path': out_path,
                        'size': deleted_size
                    })
                    deleted_filtered = update_filtered(session, parameters={
                        'data': job,
                        'filters': filters_str,
                        'storage': deleted_storage
                    })
                    job.filter_state = models.Data.FILTER_PENDING
                    session.add(clean_storage)
                    session.add(deleted_storage)
                    session.add(clean_filtered)
                    session.add(deleted_filtered)
                    session.add(job)
                    session.commit()
                    session.close()
                    processed_data = pathlib.Path(DATA_ROOT).joinpath(ARCHIVE, PROCESSED_PREFIX, out_path)
                    processed_data.parent.mkdir(parents=True, exist_ok=True)
                    unprocessed_data.replace(processed_data)
                    logging.info(f'Job '
                                 f'{colorama.Back.GREEN}{colorama.Fore.BLACK}'
                                 f'succeeded'
                                 f'{colorama.Fore.RESET}{colorama.Back.RESET}'
                                 f'.')
                    break
                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except Exception as e:
                    if tries < RETRIES:
                        logging.error(f'{colorama.Fore.LIGHTRED_EX}'
                                      f'An error has occurred: {e}'
                                      f'{colorama.Fore.RESET}')
                        logging.info(f'Retry after {RETRY_INTERVAL} seconds ({RETRIES - tries} left)).')
                        time.sleep(RETRY_INTERVAL)
                        tries += 1
                    else:
                        job = find_job_by_prefix_and_out_path(session=session, prefix=DATA_PREFIX, out_path=out_path)
                        job.filter_state = models.Data.FILTER_FAILED
                        logging.error(f'Job '
                                      f'{colorama.Back.RED}'
                                      f'failed'
                                      f'{colorama.Back.RESET}'
                                      f'.')
                        break

            session.add(job)
            session.commit()
            session.close()

        except KeyboardInterrupt:
            job = find_job_by_prefix_and_out_path(session=session, prefix=DATA_PREFIX, out_path=out_path)
            job.filter_state = models.Data.FILTER_PENDING
            logging.warning(f'Job '
                            f'{colorama.Back.YELLOW}{colorama.Fore.BLACK}'
                            f'cancelled'
                            f'{colorama.Fore.RESET}{colorama.Back.RESET}'
                            f'.')
            session.add(job)
            session.commit()
            session.close()
            return


if __name__ == '__main__':
    config = configs.config(CONFIG_PATH)
    DB_CONF = db.get_database_config(config)
    RETRY_INTERVAL = config.getint('worker', 'retry_interval')
    RETRIES = config.getint('worker', 'retries')
    SOCKET_TIMEOUT = config.getint('worker', 'socket_timeout')
    DEVICE = config.get('worker', 'device')
    FILTERS = pyjson.loads(config.get('worker', 'filters'))
    DATA_ROOT = config.get('worker', 'data_root')
    ARCHIVE = config.get('worker', 'archive')
    DATA_PREFIX = config.get('worker', 'data_prefix')
    PROCESSED_PREFIX = config.get('worker', 'processed_prefix')
    FILTERED_CLEAN_PREFIX = config.get('worker', 'filtered_clean_prefix')
    FILTERED_DELETED_PREFIX = config.get('worker', 'filtered_deleted_prefix')

    filters_str = '-'.join([str(f) for f in FILTERS])
    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
