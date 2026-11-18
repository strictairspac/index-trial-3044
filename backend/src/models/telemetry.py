from sqlalchemy import Column,Integer,BigInteger,String,Float,text
from sqlalchemy.dialects.mysql import DATETIME
from src.config.database import Base
class Telemetry(Base):
 __tablename__='telemetry'
 id=Column(Integer,primary_key=True)
 sensor_id=Column(Integer,nullable=False)
 recorded_at=Column(String(30),nullable=False)
 device_type=Column(String(40),nullable=False)
 value=Column(Float,nullable=False)
 updated_at=Column(DATETIME(fsp=6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)'))
class TelemetryArchive(Base):
 __tablename__='telemetry_archive'
 __table_args__={'mysql_auto_increment':'1000000000000'}
 id=Column(BigInteger,primary_key=True)
 sensor_id=Column(Integer,nullable=False)
 recorded_at=Column(String(30),nullable=False)
 device_type=Column(String(40),nullable=False)
 value=Column(Float,nullable=False)
 updated_at=Column(DATETIME(fsp=6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6)'))
