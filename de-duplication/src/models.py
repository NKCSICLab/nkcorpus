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
    uri = Column(String(256))
    filtered_state = Column(SmallInteger, nullable=False, default=0)

    data = relationship('Data', back_populates='process')
    worker = relationship('Worker', back_populates='process')
    filtered = relationship('Filtered', back_populates='process')
    # filter_file_proc = relationship('FilterFileProc', back_populates='process')


class Worker(Base):
    __tablename__ = 'worker'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)

    data = relationship('Data', back_populates='worker')
    process = relationship('Process', back_populates='worker')
    filtered = relationship('Filtered', back_populates='worker')
    deduped = relationship('Deduped', back_populates='worker')


class FilterProc(Base):
    __tablename__ = 'filter_proc'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    parameter = Column(String(256))


class Filtered(Base):
    __tablename__ = 'filtered'
    DEDUPED_PENDING = 0
    DEDUPED_PROCESSING = 1
    DEDUPED_FINISHED = 2


    id = Column(Integer, primary_key=True, autoincrement=True)
    id_process = Column(Integer, ForeignKey('process.id'), nullable=False)
    clean_size = Column(Integer, nullable=False, default=0)
    deleted_size = Column(Integer, nullable=False, default=0)
    filtered_at = Column(DateTime)
    id_worker = Column(Integer, ForeignKey('worker.id'))
    uri = Column(String(256))
    bit_filter = Column(Integer, nullable=False)
    deduped_state = Column(SmallInteger, nullable=False, default=0)

    process = relationship('Process', back_populates='filtered')
    worker = relationship('Worker', back_populates='filtered')
    deduped = relationship('Deduped', back_populates='filtered')

class Deduped(Base):
    __tablename__ = 'deduped'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_filtered = Column(Integer, ForeignKey('filtered.id'), nullable=False)
    uni_size = Column(Integer, nullable=False, default=0)
    dup_size = Column(Integer, nullable=False, default=0)
    deduped_at = Column(DateTime)
    id_worker = Column(Integer, ForeignKey('worker.id'))
    uri = Column(String(256))

    filtered = relationship('Filtered', back_populates='deduped')
    worker = relationship('Worker', back_populates='deduped')


# class FilterFileProc(Base):
#     __tablename__ = 'filter_file_proc'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     id_process = Column(Integer, ForeignKey('process.id'), nullable=False)
#     filter_proc = Column(Integer, nullable=False, default=0)
#
#     process = relationship('Process', back_populates='filter_file_proc')
