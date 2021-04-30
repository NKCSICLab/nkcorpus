import sys
import wget
import time
import pytz
import socket
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
URL_BASE = 'https://commoncrawl.s3.amazonaws.com'
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


def check_schedule(start_time: str, end_time: str, enabled: bool = True):
    if enabled:
        logging.info(f'Download schedule: '
                     f'{colorama.Fore.LIGHTMAGENTA_EX}'
                     f'{{start_time={start_time}, end_time={end_time}}}'
                     f'{colorama.Fore.RESET}'
                     f'.')
        while True:
            now = datetime.datetime.now(tz=pytz.timezone(TIMEZONE)).strftime('%H:%M:%S')
            if start_time <= end_time:
                if now < start_time or now > end_time:
                    time.sleep(SCHEDULE_RETRY_INTERVAL)
                else:
                    break
            else:
                if end_time < now < start_time:
                    time.sleep(SCHEDULE_RETRY_INTERVAL)
                else:
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


def find_job_by_uri(session: Session, uri: str) -> models.Data:
    return session \
        .query(models.Data) \
        .filter_by(uri=uri) \
        .one()


def main():
    db_engine = db.db_connect(DB_CONF)

    while True:
        try:
            check_schedule(start_time=START_TIME, end_time=END_TIME, enabled=SCHEDULE_ENABLED)
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
                session.begin()
                job: models.Data = session \
                    .query(models.Data) \
                    .with_for_update(skip_locked=True) \
                    .filter_by(download_state=models.Data.DOWNLOAD_PENDING) \
                    .first()
                if job is None:
                    logging.info('No unclaimed job found. This program is about to exit.')
                    session.close()
                    return
                uri = job.uri
                job.started_at = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                job.download_state = models.Data.DOWNLOAD_DOWNLOADING
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

        url = f'{URL_BASE}/{uri}'
        logging.info(f'Download from '
                     f'{colorama.Fore.LIGHTCYAN_EX}'
                     f'{url}'
                     f'{colorama.Fore.RESET}')
        session = Session(bind=db_engine)

        try:
            file = pathlib.Path(DOWNLOAD_PATH).joinpath(uri)
            file.parent.mkdir(parents=True, exist_ok=True)

            tries = 0
            while True:
                try:
                    progbar = utils.DownloadProgBar()
                    wget.download(url, out=str(file), bar=progbar.update)
                    job = find_job_by_uri(session=session, uri=uri)
                    job.worker = find_worker_by_name(session=session, name=WORKER_NAME)
                    job.finished_at = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
                    job.size = int(urlopen(url).info().get('Content-Length', -1))
                    job.download_state = models.Data.DOWNLOAD_FINISHED
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
                        job.download_state = models.Data.DOWNLOAD_FAILED
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
            job.started_at = None
            job.download_state = models.Data.DOWNLOAD_PENDING
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
    SCHEDULE_ENABLED = config.getboolean('schedule', 'enabled')
    START_TIME = config.get('schedule', 'start_time')
    END_TIME = config.get('schedule', 'end_time')
    SCHEDULE_RETRY_INTERVAL = config.getint('schedule', 'retry_interval')

    colorama.init()
    logging.basicConfig(level=logging.INFO,
                        format=f'{colorama.Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{colorama.Style.RESET_ALL} %(message)s')
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    main()
