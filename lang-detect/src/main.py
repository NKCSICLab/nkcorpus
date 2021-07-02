import sys
import time
import pytz
import simdjson as json
import socket
import warcio
import random
import logging
import pathlib
import colorama
import datetime

import db
import utils
import models
import configs

from urllib.request import urlopen
from sqlalchemy.orm import Session
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


def process_data(processed_data: pathlib.Path, downloaded_data: pathlib.Path) -> int:
    data_list = []
    with open(downloaded_data, 'rb') as stream, open(processed_data, 'w') as out:
        progbar = utils.ProgBar()
        for record in warcio.ArchiveIterator(stream):
            data = utils.extract_chinese(record)
            if data != '':
                data_list.append(utils.dump_data(data, record))
            progbar.add(1)
        out.write(json.dumps({'data': data_list}))
    return processed_data.stat().st_size


def find_worker_by_name(session: Session, name: str) -> models.Worker:
    try:
        worker = session.query(models.Worker).filter_by(name=name).one()
    except NoResultFound:
        worker = models.Worker()
        worker.name = name
        session.add(worker)
        session.commit()
    return worker


def find_job_by_uri(session: Session, uri: str) -> models.Data:
    return session \
        .query(models.Data) \
        .filter_by(uri=uri) \
        .one()


def main():
    db_engine = db.db_connect(DB_CONF)
    download_path = pathlib.Path(DOWNLOAD_PATH)

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
                logging.info('Scanning download folder...')
                data_list = list(download_path.rglob('*.warc.wet.gz'))
                if len(data_list) == 0:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    return
                file = random.choice(data_list)
                uri = str(file.relative_to(DOWNLOAD_PATH).as_posix())
                session.begin()
                job: models.Data = session \
                    .query(models.Data) \
                    .with_for_update(of=models.Data, skip_locked=True) \
                    .filter_by(uri=uri,
                               process_state=models.Data.PROCESS_PENDING) \
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
                job.process_state = models.Data.PROCESS_PROCESSING
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
                    downloaded_data = pathlib.Path(DOWNLOAD_PATH).joinpath(uri)
                    processed_data = pathlib.Path(PROCESS_PATH).joinpath(uri).with_suffix('.json')
                    processed_data.parent.mkdir(parents=True, exist_ok=True)
                    size = process_data(processed_data, downloaded_data)
                    print()
                    worker = find_worker_by_name(session=session, name=WORKER_NAME)
                    processed_at = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                    job = find_job_by_uri(session, uri)
                    process = models.Process(data=job,
                                             size=size,
                                             processed_at=processed_at,
                                             worker=worker,
                                             uri=pathlib.Path(uri).with_suffix('.json'))
                    session.add(process)
                    job.process_state = models.Data.PROCESS_FINISHED
                    downloaded_data.unlink()
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
                        job.process_state = models.Data.PROCESS_FAILED
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
            job.process_state = models.Data.PROCESS_PENDING
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
    DOWNLOAD_PATH = config.get('worker', 'download_path')
    PROCESS_PATH = config.get('worker', 'process_path')

    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
