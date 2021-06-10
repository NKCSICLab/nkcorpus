from sqlalchemy import Column, Integer, String, DateTime, SmallInteger, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Data(Base):
    __tablename__ = 'data'

    FILTER_PENDING = 0
    FILTER_PROCESSING = 1
    FILTER_FINISHED = 2
    FILTER_FAILED = 3

    id = Column(Integer, primary_key=True, autoincrement=True)
    uri = Column(String(512), nullable=False, unique=True)
    id_storage = Column(Integer, ForeignKey('storage.id'))
    filter_state = Column(SmallInteger, nullable=False, default=0)
    archive = Column(String(20))
    start_deal_time = Column(DateTime)

    storage = relationship('Storage', back_populates='data')
    filtered = relationship('Filtered', back_populates='data')


class Storage(Base):
    __tablename__ = 'storage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_device = Column(Integer, ForeignKey('device.id'))
    archive = Column(String(20), nullable=False)
    out_path = Column(String(512), nullable=False, unique=True)
    size = Column(Integer, nullable=False, default=0)

    device = relationship('Device', back_populates='storage')
    data = relationship('Data', back_populates='storage')
    filtered = relationship('Filtered', back_populates='storage')
    deduped = relationship('Deduped', back_populates='storage')


class Device(Base):
    __tablename__ = 'device'

    id = Column(Integer, primary_key=True, autoincrement=True)
    device_name = Column(String(20), nullable=False, unique=True)

    storage = relationship('Storage', back_populates='device')


class Filter(Base):
    __tablename__ = 'filter'

    id = Column(Integer, primary_key=True, autoincrement=True)
    filter_name = Column(String(50), nullable=False)
    parameters = Column(String(512))


class Filtered(Base):
    __tablename__ = 'filtered'
    DEDUP_PENDING = 0
    DEDUP_PROCESSING = 1
    DEDUP_FINISHED = 2
    DEDUP_FAILED = 3

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_data = Column(Integer, ForeignKey('data.id'), nullable=False)
    filters = Column(Integer, nullable=False)
    id_storage = Column(Integer, ForeignKey('storage.id'))
    dedup_state = Column(SmallInteger, nullable=False, default=0)
    start_deal_time = Column(DateTime)

    data = relationship('Data', back_populates='filtered')
    storage = relationship('Storage', back_populates='filtered')
    deduped = relationship('Deduped', back_populates='filtered')


class Deduped(Base):
    __tablename__ = 'deduped'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_filtered = Column(Integer, ForeignKey('filtered.id'), nullable=False)
    id_storage = Column(Integer, ForeignKey('storage.id'))

    filtered = relationship('Filtered', back_populates='deduped')
    storage = relationship('Storage', back_populates='deduped')
