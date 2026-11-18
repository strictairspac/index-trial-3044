from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.business import ArchiveInput
from src.services import archive
from src.controllers.business import start_archive,resume_archive
router=APIRouter()
@router.get('/archive')
def status(u=Depends(current)):return archive.status()
@router.post('/archive/jobs')
def create(data:ArchiveInput,u=Depends(writer)):return start_archive(data,u)
@router.post('/archive/jobs/{job_id}/resume')
def resume(job_id:int,u=Depends(writer)):return resume_archive(job_id,u)
