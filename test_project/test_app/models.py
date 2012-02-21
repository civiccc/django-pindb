from django.db import models

class Router(object):
    def db_for_read(self, model, **hints):
        if model is ModelHam:
            return "ham"

    def db_for_write(self, model, **hints):
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_syncdb(slef, db, model):
        return True

class EggModel(models.Model):
    pass

class HamModel(models.Model):
    pass



