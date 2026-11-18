from sqlalchemy import Column,Integer,BigInteger,String,text
from sqlalchemy.dialects.mysql import DATETIME
from src.config.database import Base
class ArchiveState(Base):
 __tablename__='archive_state'
 id=Column(Integer,primary_key=True)
 cutoff=Column(String(30),nullable=True)
 updated_at=Column(DATETIME(fsp=6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6)'))
class ArchiveJob(Base):
 __tablename__='archive_jobs'
 id=Column(Integer,primary_key=True)
 cutoff=Column(String(30),nullable=False)
 prev_cutoff=Column(String(30),nullable=True)
 phase=Column(String(20),nullable=False,server_default='copying')
 checkpoint_id=Column(BigInteger,nullable=False,server_default=text('0'))
 total_rows=Column(Integer,nullable=False,server_default=text('0'))
 copied_rows=Column(Integer,nullable=False,server_default=text('0'))
 cleaned_rows=Column(Integer,nullable=False,server_default=text('0'))
 verify_rounds=Column(Integer,nullable=False,server_default=text('0'))
 error=Column(String(1000),nullable=True)
 created_at=Column(DATETIME(fsp=6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6)'))
 switched_at=Column(DATETIME(fsp=6),nullable=True)
 finished_at=Column(DATETIME(fsp=6),nullable=True)
