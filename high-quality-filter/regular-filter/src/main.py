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
import utils

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


def filter_data(processed_data: pathlib.Path, filters: Sequence[models.FilterProc], filtered_clean_data: pathlib.Path,
                filtered_delete_data: pathlib.Path) -> int:
    # clean_data = []
    # deleted_data = []
    with open(processed_data, 'rb') as r, open(filtered_clean_data, 'w') as w_clean, open(filtered_delete_data,
                                                                                          'w') as w_deleted:
        data = json.load(r)['data']
        clean_data, deleted_data = utils.filter_pipeline(data, filters)
        json.dump(clean_data, w_clean)
        json.dump(deleted_data, w_deleted)
    return filtered_clean_data.stat().st_size, filtered_delete_data.stat().st_size


def find_worker_by_name(session: Session, name: str) -> models.Worker:
    try:
        worker = session.query(models.Worker).filter_by(name=name).one()
    except NoResultFound:
        worker = models.Worker()
        worker.name = name
        session.add(worker)
        session.commit()
    return worker


def find_job_by_uri(session: Session, uri: str) -> models.Process:
    return session \
        .query(models.Process) \
        .filter_by(uri=uri) \
        .one()


def find_filter_by_id_list(session: Session, id_list: list) -> Sequence[models.FilterProc]:
    logging.info(f'Get Filter...')
    try:
        filters = session \
            .query(models.FilterProc) \
            .filter(models.FilterProc.id.in_(id_list)) \
            .all()
    except:
        logging.warning('Get filter failed!')
        raise Exception
    logging.info(f'Get filter succeeded!')
    return filters


def main():
    db_engine = db.db_connect(DB_CONF)
    process_path = pathlib.Path(PROCESS_PATH)
    while True:
        try:
            check_connectivity()
        except KeyboardInterrupt:
            logging.info(f'Bye.')
            return

        logging.info('Fetching a new job...')
        session = Session(bind=db_engine)
        uri = None
        tries = 0
        while True:
            try:
                logging.info('Scanning preocessed folder...')
                data_list = list(process_path.rglob('*.warc.wet.json'))
                if len(data_list) == 0:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                file = random.choice(data_list)
                uri = str(file.relative_to(PROCESS_PATH).as_posix())
                session.begin()
                job: models.Process = session \
                    .query(models.Process) \
                    .with_for_update(skip_locked=True) \
                    .filter_by(uri=uri,
                               filtered_state=models.Process.FILTER_PENDING) \
                    .first()

                if job is None:
                    session.commit()
                    session.close()
                    logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'File: '
                                    f'{colorama.Fore.RESET}'
                                    f'{colorama.Fore.LIGHTCYAN_EX}'
                                    f'{{uri={uri}}}'
                                    f'{colorama.Fore.RESET} '
                                    f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'is not in the database or is being processed by another worker.'
                                    f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds.')
                    time.sleep(RETRY_INTERVAL)
                    continue
                if_dealt: models.FilterFileProc = session \
                    .query(models.FilterFileProc) \
                    .filter_by(id_process=job.id, filter_proc=FILTER_PROC_TODO) \
                    .first()
                if if_dealt is not None:
                    session.commit()
                    session.close()
                    logging.warning(f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'File: '
                                    f'{colorama.Fore.RESET}'
                                    f'{colorama.Fore.LIGHTCYAN_EX}'
                                    f'{{uri={uri}}}'
                                    f'{colorama.Fore.RESET} '
                                    f'{colorama.Fore.LIGHTYELLOW_EX}'
                                    f'is already processed.'
                                    f'{colorama.Fore.RESET}')
                    logging.info(f'Retry after {RETRY_INTERVAL} seconds.')
                    time.sleep(RETRY_INTERVAL)
                    continue
                job.filtered_state = models.Process.FILTER_PROCESSING
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
                    processed_data = pathlib.Path(PROCESS_PATH).joinpath(uri)
                    dealt_data = pathlib.Path(DEALT_PATH).joinpath(uri)
                    dealt_data.parent.mkdir(parents=True, exist_ok=True)
                    filtered_clean_data = pathlib.Path(FILTER_CLEAN_PATH).joinpath(f"{uri}.{FILTER_PROC_TODO}")
                    filtered_delete_data = pathlib.Path(FILTER_DELETE_PATH).joinpath(f"{uri}.{FILTER_PROC_TODO}")
                    filtered_clean_data.parent.mkdir(parents=True, exist_ok=True)
                    filtered_delete_data.parent.mkdir(parents=True, exist_ok=True)
                    filters = find_filter_by_id_list(session, FILTER_PROC_ID_LIST)
                    clean_size, deleted_size = filter_data(processed_data, filters, filtered_clean_data,
                                                           filtered_delete_data)
                    worker = find_worker_by_name(session=session, name=WORKER_NAME)
                    filtered_at = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                    job = find_job_by_uri(session, uri)
                    filtered = models.Filtered(process=job,
                                               clean_size=clean_size,
                                               deleted_size=deleted_size,
                                               filtered_at=filtered_at,
                                               worker=worker,
                                               uri=pathlib.Path(f"{uri}.{FILTER_PROC_TODO}"),
                                               bit_filter=FILTER_PROC_TODO
                                               )
                    filter_file_proc = models.FilterFileProc(process=job,
                                                             filter_proc=FILTER_PROC_TODO)
                    session.add(filtered)
                    session.add(filter_file_proc)
                    job.filtered_state = models.Process.FILTER_PENDING
                    processed_data.replace(dealt_data)
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
                        job = find_job_by_uri(session=session, uri=uri)
                        job.filtered_state = models.Process.FILTER_FAILED
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
            job = find_job_by_uri(session=session, uri=uri)
            job.filtered_state = models.Process.FILTER_PENDING
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
    RETRY_INTERVAL = config.getint('worker', 'retry_interval')
    RETRIES = config.getint('worker', 'retries')
    SOCKET_TIMEOUT = config.getint('worker', 'socket_timeout')
    PROCESS_PATH = config.get('worker', 'process_path')
    DEALT_PATH = config.get('worker', 'dealt_path')
    FILTER_CLEAN_PATH = config.get('worker', 'filter_save_path')
    FILTER_DELETE_PATH = config.get('worker', 'filter_del_path')
    DIRTY_TABLE = config.get('worker', 'dirty_table')
    # 目前设定最多32种处理方式
    FILTER_PROC_TODO = int(config.get('worker', 'filter_proc_id_bit'), 2)
    FILTER_PROC_ID_LIST = [i + 1 for i in range(32) if ((2 ** i) & FILTER_PROC_TODO)]
    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
