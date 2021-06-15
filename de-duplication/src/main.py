import datetime
import logging
import pathlib
import socket
from typing import Sequence
from urllib.request import urlopen

import colorama
import pytz
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

import configs
import db
import models
from utils import *
import sys
import time

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


def find_device_by_name(session: Session, name: str) -> models.Device:
    try:
        device = session.query(models.Device).filter_by(device_name=name).one()
    except NoResultFound:
        device = models.Device()
        device.device_name = name
        session.add(device)
        session.commit()
    return device


def find_filtered_jobs_by_path_list(session: Session, prefix: str, out_path_list: list) -> Sequence[models.Filtered]:
    jobs: Sequence[models.Filtered] = session \
        .query(models.Filtered) \
        .join(models.Storage) \
        .with_for_update(skip_locked=True) \
        .filter(models.Storage.prefix == prefix,
                models.Storage.out_path.in_(out_path_list)) \
        .all()
    return jobs


def find_filtered_job_by_path(session: Session, prefix: str, out_path: str) -> models.Filtered:
    job: models.Filtered = session \
        .query(models.Filtered) \
        .join(models.Storage) \
        .with_for_update(skip_locked=True) \
        .filter(models.Storage.prefix == prefix,
                models.Storage.out_path == out_path) \
        .one()
    return job


def find_storage_by_filtered(session: Session, filtered: models.Filtered) -> models.Storage:
    storage: models.Storage = session \
        .query(models.Storage) \
        .join(models.Filtered) \
        .filter(models.Filtered.id == filtered.id) \
        .one()
    return storage


def main():
    db_engine = db.db_connect(DB_CONF)
    if IF_ARCHIVE:
        to_de_dup_path = pathlib.Path(ARCHIVE).joinpath(TO_DE_DUP_PREFIX)
        de_duped_backup_path = pathlib.Path(ARCHIVE).joinpath(DE_DUPED_BACKUP_PREFIX)
        no_dup_path = pathlib.Path(ARCHIVE).joinpath(NO_DUP_PREFIX)
        dup_path = pathlib.Path(ARCHIVE).joinpath(DUP_PREFIX)
    else:
        to_de_dup_path = pathlib.Path(TO_DE_DUP_PREFIX)
        de_duped_backup_path = pathlib.Path(DE_DUPED_BACKUP_PREFIX)
        no_dup_path = pathlib.Path(NO_DUP_PREFIX)
        dup_path = pathlib.Path(DUP_PREFIX)

    while True:
        try:
            check_connectivity()
        except KeyboardInterrupt:
            logging.info(f'Bye.')
            return

        logging.info('Fetching a new job...')
        session = Session(bind=db_engine)
        tries = 0
        out_path_in_job = []
        while True:
            try:
                logging.info('Scanning preocessed folder...')
                data_list = list(to_de_dup_path.rglob('*.warc.wet.json'))
                if len(data_list) == 0:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                out_path_list_ = []
                for file in data_list:
                    out_path_list_.append(file.relative_to(to_de_dup_path).as_posix())
                session.begin()
                jobs = find_filtered_jobs_by_path_list(session, TO_DE_DUP_PREFIX, out_path_list_)
                if jobs is None:
                    session.commit()
                    session.close()
                    logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'File: '
                                    f'{colorama.Fore.RESET}'
                                    f'{colorama.Fore.LIGHTCYAN_EX}'
                                    f'{{prefix={TO_DE_DUP_PREFIX}}}'
                                    f'{{out_path={out_path_list_}}}'
                                    f'{colorama.Fore.RESET} '
                                    f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'is not in the database or is being processed by another worker.'
                                    f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds.')
                    time.sleep(RETRY_INTERVAL)
                    continue

                for job in jobs:
                    job.dedup_state = models.Filtered.DEDUP_PROCESSING
                    job.start_deal_time = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                    out_path_in_job.append(job.out_path)
                session.add_all(jobs)
                session.commit()
                logging.info(f'New jobs fetched: '
                             f'{colorama.Fore.LIGHTCYAN_EX}'
                             f'{{prefix={TO_DE_DUP_PREFIX}}}'
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
                to_de_dup_data_path_list = []
                processed_de_dup_data_path_list = []
                no_dup_data_path_list = []
                dup_data_path_list = []
                try:
                    for out_path in out_path_in_job:
                        to_de_dup_data_path = pathlib.Path(to_de_dup_path).joinpath(out_path)
                        processed_de_dup_data_path = pathlib.Path(de_duped_backup_path).joinpath(out_path)
                        no_dup_data_path = pathlib.Path(no_dup_path).joinpath(out_path)
                        dup_data_path = pathlib.Path(dup_path).joinpath(out_path)

                        to_de_dup_data_path_list.append(to_de_dup_data_path)
                        processed_de_dup_data_path_list.append(processed_de_dup_data_path)
                        # no_dup_data_path_list.append(no_dup_data_path)
                        # dup_data_path_list.append(dup_data_path)

                        to_de_dup_data_path.parent.mkdir(parents=True, exist_ok=True)
                        processed_de_dup_data_path.parent.mkdir(parents=True, exist_ok=True)
                        no_dup_data_path.parent.mkdir(parents=True, exist_ok=True)
                        dup_data_path.parent.mkdir(parents=True, exist_ok=True)

                    de_dup_pipeline(to_de_dup_data_path_list, to_de_dup_path, no_dup_path, dup_path, CHAR_NGRAM, SEEDS,
                                    BANDS, HASHBYTES, JAC_THRED, JAC_BAIKE_THRED)
                    # todo
                    conn_str = f"mongodb://lidongwen:Lidongwen_2021@10.10.1.217:27017/ldw_test"
                    # set a 5-second connection timeout
                    client: pymongo.MongoClient = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
                    # job = find_job_by_prefix_out_path(session=session, prefix=DATA_PATH, out_path=out_path)
                    # device = find_device_by_name(session=session, name=DEVICE)
                    # clean_storage = models.Storage(
                    #     device=device,
                    #     archive=job.archive,
                    #     prefix=pathlib.Path(FILTER_CLEAN_PATH).joinpath(str(FILTER_PROC_TODO)).as_posix(),
                    #     out_path=pathlib.Path(out_path).as_posix(),
                    #     size=clean_size
                    # )
                    # session.add(clean_storage)
                    # clean_filtered = models.Filtered(data=job,
                    #                                  filters=FILTER_PROC_TODO,
                    #                                  storage=clean_storage
                    #                                  )
                    #
                    # session.add(clean_filtered)
                    # deleted_storage = models.Storage(
                    #     device=device,
                    #     archive=job.archive,
                    #     prefix=pathlib.Path(FILTER_DELETE_PATH).joinpath(str(FILTER_PROC_TODO)).as_posix(),
                    #     out_path=pathlib.Path(out_path).as_posix(),
                    #     size=deleted_size
                    # )
                    # session.add(deleted_storage)
                    # deleted_filtered = models.Filtered(data=job,
                    #                                    filters=FILTER_PROC_TODO,
                    #                                    storage=deleted_storage
                    #                                    )
                    #
                    # session.add(deleted_filtered)
                    # job.filter_state = models.Data.FILTER_PENDING
                    # to_filter_data.replace(dealt_data)
                    # todo over
                    logging.warning(f'=====ENTERING CRITICAL ZONE=====')
                    logging.warning(f'Do not interrupt this process!')
                    for o_data, n_data in zip(to_de_dup_data_path_list, processed_de_dup_data_path_list):
                        o_data.replace(n_data)
                    logging.warning(f'=====EXITING CRITICAL ZONE=====')
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
                        jobs = find_filtered_jobs_by_path_list(session=session, prefix=TO_DE_DUP_PREFIX,
                                                               out_path=out_path_in_job)
                        for job in jobs:
                            job.filter_state = models.Data.FILTER_FAILED
                        logging.error(f'Job '
                                      f'{colorama.Back.RED}'
                                      f'failed'
                                      f'{colorama.Back.RESET}'
                                      f'.')
                        break

            session.add_all(jobs)
            session.commit()
            session.close()

        except KeyboardInterrupt:
            jobs = find_filtered_jobs_by_path_list(session=session, prefix=TO_DE_DUP_PREFIX, out_path=out_path_in_job)
            for job in jobs:
                job.filter_state = models.Data.FILTER_FINISHED
            logging.warning(f'Job '
                            f'{colorama.Back.YELLOW}{colorama.Fore.BLACK}'
                            f'cancelled'
                            f'{colorama.Fore.RESET}{colorama.Back.RESET}'
                            f'.')
            session.add_all(jobs)
            session.commit()
            session.close()
            return


if __name__ == '__main__':
    config = configs.config(CONFIG_PATH)
    DB_CONF = db.get_database_config(config)
    MONGO_DB_CONF =
    DEVICE = config.get('worker', 'device')
    RETRY_INTERVAL = config.getint('worker', 'retry_interval')
    RETRIES = config.getint('worker', 'retries')
    SOCKET_TIMEOUT = config.getint('worker', 'socket_timeout')
    IF_ARCHIVE = config.getboolean('worker', 'if_use_archive')
    ARCHIVE = config.get('worker', 'archive')

    TO_DE_DUP_PREFIX = config.get('worker', 'to_de_dup_path')
    NO_DUP_PREFIX = config.get('worker', 'no_dup_path')
    DE_DUPED_BACKUP_PREFIX = config.get('worker', 'de_duped_backup_path')
    DUP_PREFIX = config.get('worker', 'dup_path')
    CHAR_NGRAM = config.getint('minhash', 'char_ngram')
    SEEDS = config.getint('minhash', 'seeds')
    BANDS = config.getint('minhash', 'bands')
    HASHBYTES = config.getint('minhash', 'hashbytes')
    JAC_THRED = config.getfloat('jaccrad', 'jac_thred')
    JAC_BAIKE_THRED = config.getfloat('jaccrad', 'jac_baike_thred')
    MONGODB_USERNAME = config.get('mongo_db', 'username')
    username = lidongwen
    password = Lidongwen_2021
    host = 10.10
    .1
    .217
    port = 27017
    database = ldw_test
    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
