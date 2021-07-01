import datetime
import json
import logging
import pathlib
import random
import socket
import sys
import time
from typing import Sequence
from urllib.request import urlopen

import colorama
import pytz
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

import configs
import db
import models
import utils_w_delete

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


def filter_data(processed_data: pathlib.Path, filters: Sequence[models.Filter], filtered_clean_data: pathlib.Path,
                filtered_delete_data: pathlib.Path) -> int:
    # clean_data = []
    # deleted_data = []
    with open(processed_data, 'rb') as r, open(filtered_clean_data, 'w') as w_clean, open(filtered_delete_data,
                                                                                          'w') as w_deleted:
        data = json.load(r)['data']
        clean_data, deleted_data = utils_w_delete.filter_pipeline(data, filters)
        json.dump(clean_data, w_clean)
        json.dump(deleted_data, w_deleted)
    return filtered_clean_data.stat().st_size, filtered_delete_data.stat().st_size


def find_device_by_name(session: Session, name: str) -> models.Device:
    try:
        device = session.query(models.Device).filter_by(device_name=name).one()
    except NoResultFound:
        device = models.Device()
        device.device_name = name
        session.add(device)
        session.commit()
    return device


def find_job_by_prefix_out_path(session: Session, prefix: str, out_path: str) -> models.Data:
    return session \
        .query(models.Data) \
        .join(models.Storage) \
        .filter(models.Storage.prefix == prefix,
                models.Storage.out_path == out_path) \
        .one()


def find_filter_by_id_list(session: Session, id_list: list) -> Sequence[models.Filter]:
    logging.info(f'Get Filter...')
    try:
        filters = session \
            .query(models.Filter) \
            .filter(models.Filter.id.in_(id_list)) \
            .all()
    except:
        logging.warning('Get filter failed!')
        raise Exception
    logging.info(f'Get filter succeeded!')
    return filters


def update_storage(session: Session, parameters: dict) -> models.Storage:
    try:
        storage: models.Storage = session \
            .query(models.Storage) \
            .filter(models.Storage.archive == parameters["archive"],
                    models.Storage.prefix == parameters["prefix"],
                    models.Storage.out_path == parameters["out_path"]) \
            .one()
        storage.device = parameters["device"]
        storage.size = parameters["size"]
    except NoResultFound:
        storage: models.Storage = models.Storage(
            device=parameters["device"],
            archive=parameters["archive"],
            prefix=parameters["prefix"],
            out_path=parameters["out_path"],
            size=parameters["size"]
        )
    return storage


def update_filtered(session: Session, parameters: dict) -> models.Filtered:
    try:
        filtered: models.Filtered = session \
            .query(models.Filtered) \
            .filter(models.Filtered.id_data == parameters["data"].id,
                    models.Filtered.filters == parameters["filters"],
                    models.Filtered.storage == parameters["storage"].id) \
            .one()
        filtered.data = parameters["data"]
        filtered.filters = parameters["filters"]
        filtered.storage = parameters["storage"]
    except NoResultFound:
        filtered: models.Filtered = models.Filtered(
            data=parameters["data"],
            filters=parameters["filters"],
            storage=parameters["storage"]
        )
    return filtered


def main():
    db_engine = db.db_connect(DB_CONF)
    data_path = pathlib.Path(DEVICE_PATH_PREFIX).joinpath(ARCHIVE, DATA_PATH)
    while True:
        try:
            check_connectivity()
        except KeyboardInterrupt:
            logging.info(f'Bye.')
            return

        logging.info('Fetching a new job...')
        session = Session(bind=db_engine)
        tries = 0
        while True:
            try:
                logging.info('Scanning preocessed folder...')
                data_list = list(data_path.rglob('*.warc.wet.json'))
                if len(data_list) == 0:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                file = random.choice(data_list)
                out_path = str(file.relative_to(data_path).as_posix())
                session.begin()
                job: models.Data = session \
                    .query(models.Data) \
                    .join(models.Storage) \
                    .with_for_update(skip_locked=True) \
                    .filter(models.Storage.prefix == DATA_PATH,
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
                                    f'{{prefix={DATA_PATH}}}'
                                    f'{{out_path={out_path}}}'
                                    f'{colorama.Fore.RESET} '
                                    f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'is not in the database or is being processed by another worker.'
                                    f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds.')
                    time.sleep(RETRY_INTERVAL)
                    continue
                if_dealt: models.Filtered = session \
                    .query(models.Filtered) \
                    .filter_by(id_data=job.id,
                               filters=FILTER_PROC_TODO) \
                    .first()
                if if_dealt is not None:
                    session.commit()
                    session.close()
                    logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'File: '
                                    f'{colorama.Fore.RESET}'
                                    f'{colorama.Fore.LIGHTCYAN_EX}'
                                    f'{{prefix={DATA_PATH}}}'
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
                    to_filter_data = pathlib.Path(data_path).joinpath(out_path)
                    dealt_data = pathlib.Path(DEVICE_PATH_PREFIX).joinpath(ARCHIVE, DEALT_PATH, out_path)
                    dealt_data.parent.mkdir(parents=True, exist_ok=True)
                    filtered_clean_data = pathlib.Path(DEVICE_PATH_PREFIX).joinpath(ARCHIVE, FILTER_CLEAN_PATH, str(FILTER_PROC_TODO),
                                                                         out_path)
                    filtered_delete_data = pathlib.Path(DEVICE_PATH_PREFIX).joinpath(ARCHIVE, FILTER_DELETE_PATH, str(FILTER_PROC_TODO),
                                                                          out_path)
                    filtered_clean_data.parent.mkdir(parents=True, exist_ok=True)
                    filtered_delete_data.parent.mkdir(parents=True, exist_ok=True)
                    filters = find_filter_by_id_list(session, FILTER_PROC_ID_LIST)
                    clean_size, deleted_size = filter_data(to_filter_data, filters, filtered_clean_data,
                                                           filtered_delete_data)
                    job = find_job_by_prefix_out_path(session=session, prefix=DATA_PATH, out_path=out_path)
                    device = find_device_by_name(session=session, name=DEVICE)
                    clean_storage = update_storage(session, {
                        "device": device,
                        "archive": job.archive,
                        "prefix": pathlib.Path(FILTER_CLEAN_PATH).joinpath(str(FILTER_PROC_TODO)).as_posix(),
                        "out_path": pathlib.Path(out_path).as_posix(),
                        "size": clean_size

                    })
                    session.add(clean_storage)
                    clean_filtered = update_filtered(session,
                                                     {"data": job,
                                                      "filters": FILTER_PROC_TODO,
                                                      "storage": clean_storage})
                    session.add(clean_filtered)
                    deleted_storage = update_storage(session, {
                        "device": device,
                        "archive": job.archive,
                        "prefix": pathlib.Path(FILTER_DELETE_PATH).joinpath(str(FILTER_PROC_TODO)).as_posix(),
                        "out_path": pathlib.Path(out_path).as_posix(),
                        "size": deleted_size

                    })
                    session.add(deleted_storage)
                    deleted_filtered = update_filtered(session,
                                                       {"data": job,
                                                        "filters": FILTER_PROC_TODO,
                                                        "storage": deleted_storage})
                    session.add(deleted_filtered)
                    job.filter_state = models.Data.FILTER_PENDING
                    session.add(job)
                    session.commit()
                    session.close()
                    to_filter_data.replace(dealt_data)
                    logging.info(f'Job '
                                 f'{colorama.Back.GREEN}{colorama.Fore.BLACK}'
                                 f'succeeded'
                                 f'{colorama.Fore.RESET}{colorama.Back.RESET}'
                                 f'.')

                    break
                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except Exception as e:
                    # raise e
                    if tries < RETRIES:
                        logging.error(f'{colorama.Fore.LIGHTRED_EX}'
                                      f'An error has occurred: {e}'
                                      f'{colorama.Fore.RESET}')
                        logging.info(f'Retry after {RETRY_INTERVAL} seconds ({RETRIES - tries} left)).')
                        time.sleep(RETRY_INTERVAL)
                        tries += 1
                    else:
                        job = find_job_by_prefix_out_path(session=session, prefix=DATA_PATH, out_path=out_path)
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
            job = find_job_by_prefix_out_path(session=session, prefix=DATA_PATH, out_path=out_path)
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
    WORKER_NAME = config.get('worker', 'name')
    DEVICE = config.get('worker', 'device')
    RETRY_INTERVAL = config.getint('worker', 'retry_interval')
    RETRIES = config.getint('worker', 'retries')
    SOCKET_TIMEOUT = config.getint('worker', 'socket_timeout')
    ARCHIVE = config.get('worker', 'archive')
    DATA_PATH = config.get('worker', 'data_path')
    DEALT_PATH = config.get('worker', 'dealt_path')
    FILTER_CLEAN_PATH = config.get('worker', 'filter_save_path')
    FILTER_DELETE_PATH = config.get('worker', 'filter_del_path')
    DIRTY_TABLE = config.get('worker', 'dirty_table')
    # 目前设定最多32种处理方式
    FILTER_PROC_TODO = int(config.get('worker', 'filter_proc_id_bit'), 2)
    DEVICE_PATH_PREFIX = config.get('worker', 'device_path_prefix')
    FILTER_PROC_ID_LIST = [i + 1 for i in range(32) if ((2 ** i) & FILTER_PROC_TODO)]

    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
