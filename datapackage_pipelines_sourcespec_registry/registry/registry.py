import json
from uuid import uuid4

import datetime
from sqlalchemy import DateTime, types
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Unicode, String, create_engine
from sqlalchemy.orm import sessionmaker

from datapackage_pipelines.specs.resolver import resolve_generator

# ## SQL DB
Base = declarative_base()


# ## Json as string Type
class JsonType(types.TypeDecorator):
    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value:
            return json.loads(value)
        else:
            return None

    def copy(self, **kw):
        return JsonType(self.impl.length)


# ## SourceSpec
class SourceSpec(Base):
    __tablename__ = 'specs'
    uuid = Column(String(128), primary_key=True)
    owner = Column(String(128))
    module = Column(Unicode)
    contents = Column(JsonType)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class SourceSpecRegistry:

    def __init__(self, db_connection_string):
        self._db_connection_string = db_connection_string
        self._engine = None
        self._session = None

    @staticmethod
    def _verify_contents(module, contents):
        generator = resolve_generator(module)
        if not generator.internal_validate(contents):
            raise ValueError("Contents invalid for module %s" % module)

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self._db_connection_string)
            Base.metadata.create_all(self._engine)
        return self._engine

    @property
    def session(self):
        if self._session is None:
            self._session = sessionmaker(bind=self.engine)()
        return self._session

    def list_source_specs(self):
        yield from self.session.query(SourceSpec).all()

    def get_source_spec(self, uuid):
        return self.session.query(SourceSpec).filter_by(uuid=uuid).first()

    def put_source_spec(self, owner, module, contents, uuid=None):

        self._verify_contents(module, contents)

        spec = None
        now = datetime.datetime.now()
        if uuid is None:
            uuid = uuid4().hex
        else:
            spec = self.get_source_spec(uuid)
        if spec is None:
            spec = SourceSpec(uuid=uuid, created_at=now)

        spec.owner = owner
        spec.module = module
        spec.contents = contents
        spec.updated_at = now

        s = self.session
        s.add(spec)
        s.commit()

        return uuid
