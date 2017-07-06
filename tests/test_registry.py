import os

import pytest

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
    registry.put_source_spec('me', 'sourcespec_registry', {})
    del registry
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    ret = list(registry.list_source_specs())
    assert len(ret) == 1
    assert ret[0].owner == 'me'
    assert ret[0].module == 'sourcespec_registry'
    assert ret[0].contents == {}


def test_addition():
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    registry.put_source_spec('me', 'sourcespec_registry', {})
    del registry
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    registry.put_source_spec('moi', 'sourcespec_registry', {'db-connection-string': '12'})
    registry.put_source_spec('mee', 'sourcespec_registry', {'db-connection-string': '13'})
    ret = list(registry.list_source_specs())
    assert len(ret) == 3
    assert [x.owner for x in ret] == ['me', 'moi', 'mee']
    assert [x.module for x in ret] == ['sourcespec_registry']*3
    assert [x.contents.get('db-connection-string') for x in ret] == [None, '12', '13']


def test_validation_fails():
    registry = SourceSpecRegistry(FIlE_DB_CONN_STR)
    with pytest.raises(ValueError):
        registry.put_source_spec('moi', 'sourcespec_registry', {'db-connection-string': 12})
