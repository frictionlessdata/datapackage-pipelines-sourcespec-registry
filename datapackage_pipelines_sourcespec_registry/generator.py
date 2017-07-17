import json
import os

from datapackage_pipelines.generators import GeneratorBase

from .registry import SourceSpecRegistry

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/sourcespec_registry_schema.json')

registries = {}


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        db_connection_string = \
            source.get('db-connection-string',
                       os.environ.get('SOURCESPEC_REGISTRY_DB_ENGINE'))
        if db_connection_string not in registries:
            registries[db_connection_string] = \
                SourceSpecRegistry(db_connection_string)
        registry = registries[db_connection_string]

        for spec in registry.list_source_specs():
            yield f':{spec.module}:', spec.contents
