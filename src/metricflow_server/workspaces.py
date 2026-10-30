"""Versioned workspace API. Reuses one adapter without replacing default metrics."""
from __future__ import annotations
from collections import OrderedDict
from dataclasses import dataclass,field
import copy
import datetime
import hashlib
import json
import re
import threading
import time
from typing import Literal
from fastapi import APIRouter,Depends,HTTPException
from pydantic import BaseModel,Field,field_validator,model_validator
from metricflow_server.auth import verify_api_key,verify_admin_key
from metricflow_server.engine_manager import engine_manager
from metricflow_server.semantic_catalog import catalog,execute_query

api_router=APIRouter(prefix='/api/v1/workspaces',tags=['workspace semantics'])
admin_router=APIRouter(prefix='/admin/workspaces',tags=['workspace semantics'])


@dataclass
class Revision:
    manifest:dict
    subjects:list
    engine:object
    digest:str
    catalogs:dict=field(default_factory=dict)
    lock:threading.Lock=field(default_factory=threading.Lock)


class Registry:
    def __init__(self,max_revisions=16):
        self.entries=OrderedDict()
        self.lock=threading.RLock()
        self.max_revisions=max_revisions

    def get(self,workspace,revision):
        with self.lock:
            key=(workspace,revision)
            if key not in self.entries:raise HTTPException(404,'Workspace revision not loaded')
            self.entries.move_to_end(key)
            return self.entries[key]

    def register(self,workspace,revision,manifest,subjects):
        content=json.dumps(manifest,ensure_ascii=False,sort_keys=True)
        digest=hashlib.sha256((content+json.dumps(subjects,sort_keys=True,ensure_ascii=False)).encode()).hexdigest()
        # Single-flight construction. Existing in-flight requests keep their old
        # revision object even if it is later evicted by LRU.
        with self.lock:
            key=(workspace,revision)
            if key in self.entries:
                if self.entries[key].digest!=digest:raise HTTPException(409,'Revision already exists with different content')
                self.entries.move_to_end(key)
                return True
            engine=engine_manager.create_engine(content)
            self.entries[key]=Revision(manifest,subjects,engine,digest)
            while len(self.entries)>self.max_revisions:self.entries.popitem(last=False)
            return False

    def metadata(self,entry,metrics,force=False):
        key=tuple(sorted(set(metrics)))
        with entry.lock:
            if key in entry.catalogs and not force:return copy.deepcopy(entry.catalogs[key]),True
            result=catalog(entry.engine,entry.manifest,list(key),entry.subjects)
            entry.catalogs[key]=copy.deepcopy(result)
            if len(entry.catalogs)>128:entry.catalogs.pop(next(iter(entry.catalogs)))
            return result,False


registry=Registry()


def identity(workspace,revision):
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,63}',workspace) or not re.fullmatch(r'[a-f0-9]{64}',revision):
        raise HTTPException(422,'Invalid workspace or revision identity')


class Subject(BaseModel):
    id:str=Field(min_length=1,max_length=160)
    name:str=Field(max_length=500)


class ManifestUpload(BaseModel):
    manifest:dict
    subjects:list[Subject]=Field(default_factory=list,max_length=5000)


class SemanticRequest(BaseModel):
    action:Literal['dimensions','compile','query']='dimensions'
    metrics:list[str]=Field(min_length=1,max_length=8)
    group_by:list[str]=Field(default_factory=list,max_length=20)
    where:str=Field(default='',max_length=4000)
    start:datetime.date|None=None
    end:datetime.date|None=None
    limit:int=Field(default=100,ge=1,le=1000)
    force:bool=False
    order_by:list[str]=Field(default_factory=list,max_length=20)

    @field_validator('order_by')
    @classmethod
    def valid_order(cls,values):
        if any(not re.fullmatch(r'-?[a-zA-Z_][a-zA-Z0-9_]*',v) for v in values):raise ValueError('Invalid sort field')
        return values

    @field_validator('metrics','group_by')
    @classmethod
    def valid_names(cls,values):
        if any(not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*',v) for v in values):raise ValueError('Invalid semantic name')
        return values

    @field_validator('where')
    @classmethod
    def predicate(cls,value):
        if any(s in value for s in (';','--','/*')):raise ValueError('Only one filter expression is allowed')
        return value

    @model_validator(mode='after')
    def valid_dates(self):
        if self.start and self.end and self.start>self.end:raise ValueError('Start must not be after end')
        return self


@api_router.get('/health')
def workspace_health():
    if not engine_manager.adapter_ready:raise HTTPException(503,'Adapter not ready')
    return dict(status='ready',capability='versioned-workspaces-v1')


@api_router.get('/{workspace}/revisions/{revision}',dependencies=[Depends(verify_api_key)])
def revision_status(workspace:str,revision:str):
    identity(workspace,revision)
    registry.get(workspace,revision)
    return dict(status='ready',workspace=workspace,revision=revision)


@admin_router.put('/{workspace}/revisions/{revision}',dependencies=[Depends(verify_admin_key)])
def upload(workspace:str,revision:str,body:ManifestUpload):
    identity(workspace,revision)
    if not engine_manager.adapter_ready:raise HTTPException(503,'Adapter not ready')
    try:reused=registry.register(workspace,revision,body.manifest,[s.model_dump() for s in body.subjects])
    except HTTPException:raise
    except Exception as exc:raise HTTPException(422,'Invalid semantic manifest: '+str(exc)) from exc
    return dict(status='ready',workspace=workspace,revision=revision,reused=reused)


@api_router.post('/{workspace}/revisions/{revision}/execute',dependencies=[Depends(verify_api_key)])
def execute(workspace:str,revision:str,body:SemanticRequest):
    identity(workspace,revision)
    entry=registry.get(workspace,revision)
    started=time.perf_counter()
    try:
        result,cached=registry.metadata(entry,body.metrics,body.force)
        if body.action!='dimensions':
            payload=body.model_dump(mode='json',exclude_none=True)
            with entry.lock:result.update(execute_query(entry.engine,payload))
        return dict(ok=True,**result,catalog_cached=cached,api_ms=round((time.perf_counter()-started)*1000,2))
    except Exception as exc:
        raise HTTPException(400,str(exc)) from exc
