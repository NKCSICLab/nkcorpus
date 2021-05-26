from sqlalchemy import Column, Integer, String, DateTime, SmallInteger, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Data(Base):
    __tablename__ = 'data'

    DOWNLOAD_PENDING = 0
    DOWNLOAD_DOWNLOADING = 1
    DOWNLOAD_FINISHED = 2
    DOWNLOAD_FAILED = 3

    PROCESS_PENDING = 0
    PROCESS_PROCESSING = 1
    PROCESS_FINISHED = 2
    PROCESS_FAILED = 3

    id = Column(Integer, primary_key=True, autoincrement=True)
    uri = Column(String(256), nullable=False)
    size = Column(Integer, nullable=False, default=0)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    process_state = Column(SmallInteger, nullable=False, default=0)
    download_state = Column(SmallInteger, nullable=False, default=0)
    id_worker = Column(Integer, ForeignKey('worker.id'))
    archive = Column(String(30))

    worker = relationship('Worker', back_populates='data')
    process = relationship('Process', uselist=False, back_populates='data')


class Process(Base):
    __tablename__ = 'process'

    FILTER_PENDING = 0
    FILTER_PROCESSING = 1
    FILTER_FINISHED = 2
    FILTER_FAILED = 3

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_data = Column(Integer, ForeignKey('data.id'), nullable=False)
    size = Column(Integer, nullable=False, default=0)
    processed_at = Column(DateTime)
    id_worker = Column(Integer, ForeignKey('worker.id'))
    filter_state = Column(SmallInteger, nullable=False, default=0)
    uri = Column(String(256))

    data = relationship('Data', back_populates='process')
    worker = relationship('Worker', back_populates='process')
    after_filter = relationship('AfterFilter', back_populates='process')


class Worker(Base):
    __tablename__ = 'worker'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)

    data = relationship('Data', back_populates='worker')
    process = relationship('Process', back_populates='worker')

class Filter(Base):
    __tablename__ = 'filter'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    parameter = Column(String(256))
    base_path = Column(String(64))
    explain = Column(String(256))

    filtered_data = relationship('FilteredData', back_populates='filter')

class FilteredData(Base):
    __tablename__ = 'filtered_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_process = Column(Integer, ForeignKey('process.id'), nullable=False)
    id_filter = Column(Integer, ForeignKey('filter.id'), nullable=False)

    process = relationship('Process', back_populates='filtered_data')
    filter = relationship('Filter', back_populates='filtered_data')

class AfterFilter(Base):
    __tablename__ = 'after_filter'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_process = Column(Integer, ForeignKey('process.id'), nullable=False)
    size = Column(Integer, nullable=False, default=0)
    filtered_at = Column(DateTime)
    id_worker = Column(Integer, ForeignKey('worker.id'))
    uri = Column(String(256))

    process = relationship('Process', back_populates='after_filter')
    filtered_data = relationship('FilteredData', back_populates='after_filter')


