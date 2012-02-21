__version__  =  (0, 1, 0) # remember to change setup.py

from django.utils.datastructures import SortedDict
def populate_replicas(masters, slave_overrides):
    replica_template = "%s-%s"
    ret = {}
    for alias, master_values in masters.items():
        ret[alias] = master_values
        for i, slave_overrides in enumerate(slave_overrides[alias]):
            replica_alias = replica_template % (alias, i)
            replica_settings = master_values.copy()
            replica_settings.update(slave_overrides)
            ret[replica_alias] = replica_settings

    if not "default" in ret:
        if not isinstance(masters, SortedDict):
            raise ValueError("Either name a master default or make MASTER_DATABASES stable with SortedDict.")
        ret["default"] = masters[masters.keys()[0]]
    return ret
