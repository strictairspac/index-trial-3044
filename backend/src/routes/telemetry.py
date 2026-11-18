from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.business import TelemetryInput,TelemetryCorrect
from src.services import telemetry_store
from src.controllers.business import backfill,correct
router=APIRouter()
@router.get('/telemetry')
def listing(sensor_id:int|None=None,start:str|None=None,end:str|None=None,limit:int=50,u=Depends(current)):
 return telemetry_store.list_rows(sensor_id,start,end,min(limit,200))
@router.post('/telemetry')
def create(data:TelemetryInput,u=Depends(writer)):return backfill(data,u)
@router.put('/telemetry/{item_id}')
def update(item_id:int,data:TelemetryCorrect,u=Depends(writer)):return correct(item_id,data,u)
