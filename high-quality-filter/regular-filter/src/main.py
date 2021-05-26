import sys
import time
import pytz
import json
import socket
import warcio
import logging
import pathlib
import colorama
import datetime

import db
import utils
import models
import configs
import argparse

from urllib.request import urlopen
from sqlalchemy.orm import Session, Query
from sqlalchemy.exc import NoResultFound

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


def filter_data(filtered_data: pathlib.Path, processed_data: pathlib.Path) -> int:
    data_list = []
    with open(processed_data, 'rb') as stream, open(processed_data, 'w') as out:
        progbar = utils.ProgBar()
        data = json.loads(stream)
        for record in data:
            record['data'] = utils.filter_pipline(record['data'], FILTER)
            if record['data'] != '':
                data_list.append(record)
            progbar.add(1)
        out.write(json.dumps({'data': data_list}))
    return filtered_data.stat().st_size


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

def find_filter_by_id(session: Session, id: ine) -> models.Filter:
    return session \
        .query(models.Filter) \
        .filter_by(id=id) \
        .one()

def get_filter():
    db_engine = db.db_connect(DB_CONF)
    logging.info('Get filter...')
    try:
        check_connectivity()
    except KeyboardInterrupt:
        logging.info(f'Bye.')
        return
    session.begin()
    #获取过滤器



def main():
    db_engine = db.db_connect(DB_CONF)
    process_path = pathlib.Path(DOWNLOAD_PATH)
    logging.info('Scanning process folder...')
    data_list = list(process_path.rglob('*.warc.wet.gz.json'))
    num_job = len(data_list)
    now_job = 0

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
                if now_job >= num_job:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                file = data_list[now_job]
                uri = str(file.relative_to(PROCESS_PATH).as_posix())
                session.begin()
                job: models.Process = session \
                    .query(models.Process) \
                    .with_for_update(skip_locked=True) \
                    .filter_by(uri=uri,
                            filter_state=models.Process.FILTER_PENDING) \
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
                    now_job += 1
                    continue
                job.filter_state = models.Process.FILTER_PROCESSING
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
                    filtered_data = pathlib.Path(FILTER_PATH).joinpath(uri)
                    filtered_data.parent.mkdir(parents=True, exist_ok=True)
                    size = filter_data(processed_data, downloaded_data)
                    worker = find_worker_by_name(session=session, name=WORKER_NAME)
                    filtered_at = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                    job = find_job_by_uri(session, uri)
                    after_filter = models.AfterFilter(process=job,
                                            size=size,
                                            filtered_at=filtered_at,
                                            worker=worker,
                                            uri=pathlib.Path(uri))
                    session.add(process)
                    for id in FILTER_ID:
                        filter = find_filter_by_id(session=session, id=id)
                        f_data = models.FilteredData(process=job,filter=filter)
                        session.add(f_data)
                    job.filter_state = models.Process.FILTER_FINISHED
                    processed_data.unlink()
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
                        job.filter_state = models.Process.FILTER_FAILED
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
            job.filter_state = models.Process.FILTER_PENDING
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
    FILTER_PATH = config.get('worker', 'filter_path')
    ARCHIVE = config.get('worker', 'archive')

    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filterid", nargs='+', type=int, default=[0, 1, 2, 3])
    FILTER_ID = parser.parse_args()
    FILTER = get_filter()


    main()
