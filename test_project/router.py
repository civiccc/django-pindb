from .test_app.models import HamModel, EggModel

class HamAndEggRouter(object):
    def db_for_read(self, model, **hints):        
        if model is EggModel:
            return "egg"

    def db_for_write(self, model, **hints):
        if model is EggModel:
            return "egg"

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_syncdb(self, db, model):
        if db == "egg" and model is EggModel:
            return True
        if db == "default" and model is HamModel:
            return True
        return None
