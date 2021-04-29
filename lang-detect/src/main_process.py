# coding=UTF-8
import logging
import datetime
import sys
import pytz
import pathlib
import os

sys.path.insert(0, '../../data-collection/comcrawl-downloader/src')
import configs
import db
import models
from main import find_job_by_uri,find_worker_by_name
import utils_process

from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from colorama import Fore, Back, Style
from warcio.archiveiterator import ArchiveIterator

TIMEZONE = 'Asia/Shanghai'
FOLDER_BASE = 'processed/'
FILE_TAIL = '.txt'

def mkdir(file_path):
    path = os.getcwd() + '/' + file_path
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)

def pipline(record, out): #处理流程
    data = utils_process.detect_ch(record)

    if data != '':
        utils_process.load_and_write(data, record, out)

def process_pipline(uri:str) -> int:
    split_position = uri.rfind('/')
    file_path = uri[0:split_position]
    mkdir(FOLDER_BASE+file_path)
    
    data = ''
    size = 0
    out_file = FOLDER_BASE+uri+FILE_TAIL
    with open(DOWNLOAD_PATH+uri, 'rb') as stream:
        with open(out_file,'w',encoding="utf-8") as out:
            for record in ArchiveIterator(stream):
                pipline(record, out)
    return os.path.getsize(out_file)
            

def main():
    try:
        db_engine = db.db_connect(DB_CONF)
    except:
        logging.error('Failed to connect to database')
        return
    while True:
        logging.info('Fetching a new job...')
        uri = ''
        try:
            session = Session(bind=db_engine)
            session.begin()
            if ARCHIVE_ENABLED:
                job: models.Data = session \
                    .query(models.Data) \
                    .with_for_update() \
                    .filter_by(download_state=models.Data.DOWNLOAD_FINISHED, 
                            process_state=models.Data.DOWNLOAD_PENDING,
                            archive=ARCHIVE) \
                    .first()
            else:
                job: models.Data = session \
                    .query(models.Data) \
                    .with_for_update() \
                    .filter_by(download_state=models.Data.DOWNLOAD_FINISHED, 
                            process_state=models.Data.DOWNLOAD_PENDING) \
                    .first()
            if job is None:
                logging.info('No unclaimed job found. This program is about to exit.')
                session.close()
                return
            uri = job.uri
            job.process_state = models.Data.DOWNLOAD_DOWNLOADING
            session.add(job)
            session.commit()
            logging.info(f'New job fetched: {Fore.LIGHTCYAN_EX}{{id={job.id}, uri={job.uri}}}{Fore.RESET}.')
            session.close()
        except Exception as e:
            job.process_state = models.Data.DOWNLOAD_PENDING
            session.add(job)
            session.commit()
            session.close()
            logging.error(f'{Fore.LIGHTRED_EX}An error has occurred: {e}{Fore.RESET}')
            sys.exit(1)
        
        session = Session(bind=db_engine)
        session.begin()
        try:
            size = process_pipline(uri)
            worker = find_worker_by_name(session=session, name=WORKER_NAME)
            process_time = datetime.datetime.now(tz=pytz.timezone(TIMEZONE))
            job = find_job_by_uri(session, uri)
            new_process = models.Process(id_data=job.id, 
                                        size=size, 
                                        processed_at=process_time,
                                        id_worker=worker.id,
                                        uri=(FOLDER_BASE+job.uri+FILE_TAIL))
            session.add(new_process)
            job.process_state = models.Data.DOWNLOAD_FINISHED
        except Exception as e:
            logging.error(f'{Fore.LIGHTRED_EX}An error has occurred: {e}{Fore.RESET}')
            job = find_job_by_uri(session, uri)
            job.process_state = models.Data.DOWNLOAD_FAILED
        finally:
            session.add(job)
            session.commit()
            session.close()

if __name__ == '__main__':
    config = configs.config()
    DB_CONF = db.get_database_config(config)
    ARCHIVE_ENABLED = config.get('process', 'archive_enabled')
    ARCHIVE = config.get('process', 'archive')
    WORKER_NAME = config.get('process', 'name')
    DOWNLOAD_PATH = config.get('process', 'download_path')
    logging.basicConfig(level=logging.INFO,
                        format=f'{Style.BRIGHT}[%(asctime)s] [%(levelname)8s]{Style.RESET_ALL} %(message)s')
    main()