from peewee import PostgresqlDatabase, Model, BooleanField, CharField, ForeignKeyField


# wrapper for usage of the Peewee ORM with the Postgres RDBMS
class Postgres:
    models = {}

    def __init__(self, database_name, user, password, host, port):
        db = PostgresqlDatabase(database_name, user=user, password=password, host=host, port=port)

        # define our peewee models
        class Group(Model):
            name = CharField()
            reuse = BooleanField()
            broadcast_id = CharField()

            class Meta:
                database = db
                db_table = 'groups'

        class Contact(Model):
            name = CharField()
            email = CharField()
            phone = CharField()
            sid = CharField()
            vanid = CharField()
            group_id = ForeignKeyField(Group, db_column='group_id', related_name='contacts', to_field='id')

            class Meta:
                database = db
                db_table = 'contacts'

        self.models["Group"] = Group
        self.models["Contact"] = Contact
        self.db = db

    def get_db(self):
        return self.db

    def get_model(self, model_name):
        if model_name in self.models:
            return self.models[model_name]
        raise KeyError("{0) model does not exist".format(model_name))
