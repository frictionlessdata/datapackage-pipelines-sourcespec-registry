import os

import pytest
import time

from datapackage_pipelines_sourcespec_registry.registry import SourceSpecRegistry

DB_FILE = 'db.tmp'
FIlE_DB_CONN_STR = f'sqlite:///{DB_FILE}'
MEM_DB_CONN_STR = 'sqlite://'


def setup():
    if os.path.exists(DB_FILE):
        print('DELETING')
        os.unlink(DB_FILE)


def test_creation_empty():
    registry = SourceSpecRegistry(MEM_DB_CONN_STR)
    assert list(registry.list_source_specs()) == []


def test_creation_exists():
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    registry.put_source_spec('ds0', 'me', 'sourcespec_registry', {})
    del registry
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    ret = list(registry.list_source_specs())
    assert len(ret) == 1
    assert ret[0].dataset_name == 'ds0'
    assert ret[0].owner == 'me'
    assert ret[0].module == 'sourcespec_registry'
    assert ret[0].contents == {}


def test_addition():
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    registry.put_source_spec('ds1', 'me', 'sourcespec_registry', {})
    del registry
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    time.sleep(2)
    registry.put_source_spec('ds2', 'moi', 'sourcespec_registry', {'db-connection-string': '12'})
    time.sleep(2)
    registry.put_source_spec('ds3', 'mee', 'sourcespec_registry', {'db-connection-string': '13'})
    ret = list(registry.list_source_specs())
    assert len(ret) == 3
    assert [x.dataset_name for x in ret] == ['ds3', 'ds2', 'ds1']
    assert [x.owner for x in ret] == ['mee', 'moi', 'me']
    assert [x.module for x in ret] == ['sourcespec_registry']*3
    assert [x.contents.get('db-connection-string') for x in ret] == ['13', '12', None]


def test_replacing_datasets():
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    registry.put_source_spec('ds3', 'me', 'sourcespec_registry', {})
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    registry.put_source_spec('ds3', 'me', 'sourcespec_registry', {})
    ret = list(registry.list_source_specs())
    assert len(ret) == 1
    assert ret[0].dataset_name == 'ds3'
    assert ret[0].owner == 'me'
    assert ret[0].module == 'sourcespec_registry'


def test_validation_fails():
    registry = SourceSpecRegistry(MEM_DB_CONN_STR)
    with pytest.raises(ValueError):
        registry.put_source_spec('ds4', 'moi', 'sourcespec_registry', {'db-connection-string': 12})


def test_missing_module():
    registry = SourceSpecRegistry(MEM_DB_CONN_STR)
    with pytest.raises(ImportError):
        registry.put_source_spec('ds5', 'moi', 'unknown', {'db-connection-string': 12})


def test_ignore_missing_module():
    registry = SourceSpecRegistry(MEM_DB_CONN_STR)
    registry.put_source_spec('ds6', 'moi', 'unknown', {'db-connection-string': 12}, ignore_missing=True)
    ret = list(registry.list_source_specs())
    assert len(ret) == 1
    assert ret[0].dataset_name == 'ds6'
    assert ret[0].owner == 'moi'
    assert ret[0].module == 'unknown'
    assert ret[0].contents == {'db-connection-string': 12}

