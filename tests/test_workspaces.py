from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient
from metricflow_server.main import app
from metricflow_server import workspaces as ws

REV='a'*64
BASE='/api/v1/workspaces/demo/revisions/'+REV
ADMIN='/admin/workspaces/demo/revisions/'+REV
API={'Authorization':'Bearer test-api-key'}
AUTH={'Authorization':'Bearer test-admin-key'}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(ws,'registry',ws.Registry(max_revisions=2))
    with patch('metricflow_server.engine_manager.EngineManager.init_adapter'),patch.object(ws.engine_manager,'_sql_client',object()):
        with TestClient(app) as client:yield client


def test_workspace_registration_isolated_and_idempotent(client):
    default=object()
    with patch.object(ws.engine_manager,'_engine',default),patch.object(ws.engine_manager,'create_engine',return_value=object()) as build:
        body={'manifest':{},'subjects':[{'id':'orders','name':'Orders'}]}
        assert client.put(ADMIN,json=body,headers=AUTH).json()['reused'] is False
        assert client.put(ADMIN,json=body,headers=AUTH).json()['reused'] is True
        assert build.call_count==1
        assert ws.engine_manager.engine is default
        assert client.get(BASE,headers=API).status_code==200
        assert client.put(ADMIN,json={'manifest':{'changed':True}},headers=AUTH).status_code==409


def test_catalog_cache_does_not_cache_query_rows(client):
    with patch.object(ws.engine_manager,'create_engine',return_value=object()):
        client.put(ADMIN,json={'manifest':{}},headers=AUTH)
    metadata={'dimensions':[{'name':'customer__city'}],'entities':[],'engine':'MetricFlow','metrics':['amount']}
    with patch.object(ws,'catalog',return_value=metadata) as catalog,patch.object(ws,'execute_query',side_effect=[{'rows':[{'amount':1}]},{'rows':[{'amount':2}]}]) as query:
        payload={'metrics':['amount'],'action':'dimensions'}
        assert client.post(BASE+'/execute',json=payload,headers=API).json()['catalog_cached'] is False
        assert client.post(BASE+'/execute',json=payload,headers=API).json()['catalog_cached'] is True
        assert catalog.call_count==1
        assert client.post(BASE+'/execute',json={**payload,'force':True},headers=API).json()['catalog_cached'] is False
        assert catalog.call_count==2
        payload['action']='query'
        assert client.post(BASE+'/execute',json=payload,headers=API).json()['rows']==[{'amount':1}]
        assert client.post(BASE+'/execute',json=payload,headers=API).json()['rows']==[{'amount':2}]
        assert query.call_count==2


def test_auth_identity_and_bounds(client):
    assert client.put(ADMIN,json={'manifest':{}},headers=API).status_code==401
    assert client.get(BASE,headers={'Authorization':'Bearer wrong'}).status_code==401
    assert client.get('/api/v1/workspaces/demo/revisions/not-a-hash',headers=API).status_code==422
    assert client.get(BASE,headers=API).status_code==404
    assert client.post(BASE+'/execute',json={'metrics':['amount'],'where':'1=1;drop table x'},headers=API).status_code==422
    assert client.post(BASE+'/execute',json={'metrics':['amount'],'limit':10000},headers=API).status_code==422


def test_lru_eviction_keeps_active_revision_object(client):
    with patch.object(ws.engine_manager,'create_engine',return_value=object()):
        ws.registry.register('demo',REV,{},[])
        active=ws.registry.get('demo',REV)
        ws.registry.register('demo','b'*64,{},[])
        ws.registry.register('demo','c'*64,{},[])
        assert len(ws.registry.entries)==2
        with pytest.raises(Exception):ws.registry.get('demo',REV)
        assert active.engine is not None
