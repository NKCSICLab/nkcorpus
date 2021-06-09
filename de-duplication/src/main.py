import datetime
import logging
import pathlib
import random
import socket
import sys
import time
from urllib.request import urlopen

import colorama
import pytz
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

import configs
import db
import models

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


def main():
    db_engine = db.db_connect(DB_CONF)
    filtered_path = pathlib.Path(ARCHIVE.replace('/', '-')).joinpath(FILTER_CLEAN_PATH)
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
                data_list = list(filtered_path.rglob(f'*.warc.wet.json.{FILTER_PROC_TODO}'))
                if len(data_list) == 0:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                uris = []
                for file in data_list:
                    uris.append(str(file.relative_to(f"{ARCHIVE.replace('/', '-')}/{FILTER_CLEAN_PATH}").as_posix()))
                session.begin()
                jobs: models.Filtered = session \
                    .query(models.Filtered) \
                    .with_for_update(skip_locked=True) \
                    .filter(models.Filtered.uri.in_(uris),
                            deduped_state=models.Filtered.DEDUPED_PENDING) \
                    .all()

                if jobs is None:
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
                for job in jobs:
                    jobs.deduped_state = models.Filtered.DEDUPED_PENDING
                session.add_all(jobs)
                session.commit()
                logging.info(f'New jobs fetched: '
                             f'{colorama.Fore.LIGHTCYAN_EX}'
                             # f'{{id={job.id}, uri={job.uri}}}'
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
                    # filter_file_proc = models.FilterFileProc(process=job,
                    #                                          filter_proc=FILTER_PROC_TODO)
                    session.add(filtered)
                    # session.add(filter_file_proc)
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
    FILTER_CLEAN_PATH = config.get('worker', 'filter_save_path')
    ARCHIVE = config.get('worker', 'archive')
    # 目前设定最多32种处理方式
    FILTER_PROC_TODO = int(config.get('worker', 'filter_proc_id_bit'), 2)
    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
