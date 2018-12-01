import os
from peewee import Model, BooleanField, CharField, ForeignKeyField
from playhouse.pool import PooledPostgresqlExtDatabase

postgres_db = PooledPostgresqlExtDatabase(os.environ["POSTGRES_DB"], register_hstore=False,
                                          max_connections=8, stale_timeout=300, user=os.environ["POSTGRES_USER"],
                                          password=os.environ["POSTGRES_PASSWORD"], host=os.environ["POSTGRES_HOST"],
                                          port=os.environ["POSTGRES_PORT"])


class Group(Model):
    name = CharField()
    reuse = BooleanField()
    broadcast_id = CharField()

    class Meta:
        database = postgres_db
        db_table = 'groups'


class Contact(Model):
    name = CharField()
    email = CharField()
    phone = CharField()
    sid = CharField()
    vanid = CharField()
    group_id = ForeignKeyField(Group, db_column='group_id', related_name='contacts', to_field='id')

    class Meta:
        database = postgres_db
        db_table = 'contacts'
