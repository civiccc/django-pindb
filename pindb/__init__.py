__version__  =  (0, 1, 5) # remember to change setup.py

import contextlib
from functools import wraps
from threading import local
from itertools import cycle
from random import randint
from warnings import warn

from django.conf import settings
from django.utils import importlib

from .exceptions import PinDBException, PinDBConfigError, UnpinnedWriteException

__all__ = (
    'PinDBException', 'PinDBConfigError', 'UnpinnedWriteException',
    'unpin_all', 'pin', 'get_pinned', 'get_newly_pinned',
    'is_pinned', 'get_replica', 'unpinned_replica',
    'populate_replicas', 'StrictPinDBRouter', 'GreedyPinDBRouter'
)

_locals = local()

# number of replicas for each db set, loaded when the Router is constructed;
# zero-based to ease using random.randint

def unpin_all():
    # the authoritative set of pinned alias.
    _locals.pinned_set = set()
    # the newly-pinned ones for advising the pinned context (i.e. for persistence.)
    _locals.newly_pinned = set()

def _init_state():
    unpin_all()
    _locals.DB_SET_SIZES = {}

# initialize state
_init_state()

def pin(alias, count_as_new=True):
    _locals.pinned_set.add(alias)
    if count_as_new:
        _locals.newly_pinned.add(alias)

def _unpin_one(alias):
    """
    Not intended for external use; just here for the unpinned_replica decorator below.
    """
    _locals.pinned_set.remove(alias)    

def get_pinned():
    return _locals.pinned_set.copy()

def get_newly_pinned():
    return _locals.newly_pinned.copy()

def is_pinned(alias):
    return alias in _locals.pinned_set

REPLICA_TEMPLATE = "%s-%s"
def _make_replica_alias(master_alias, replica_num):
    return REPLICA_TEMPLATE % (master_alias, replica_num)


# TODO: add an option for reading from the repliac once one is selected in a given pinning context;
#  This would allow for replicas in a given db set having different amounts of lag.
#  Otherwise we could still get inconsistent reads when round-robining among replicas.
def get_replica(master_alias):
    if _locals.DB_SET_SIZES[master_alias] == -1:
        return master_alias
    else:
        replica_num = randint(0, _locals.DB_SET_SIZES[master_alias])
        return _make_replica_alias(master_alias, replica_num)

class unpinned_replica(object):
    """
    with unpinned_replica("default"):
        ...

    Read from a replica despite pinning state.
    """
    def __init__(self, alias):
        self.alias = alias

    def __enter__(self):
        self.was_pinned = is_pinned(self.alias)
        if self.was_pinned:
            _unpin_one(self.alias)

    def __exit__(self, type, value, tb):
        if self.was_pinned:
            pin(self.alias)

        if any((type, value, tb)):
            raise type, value, tb

def _mash_aliases(aliases):
    if not isinstance(aliases, basestring) and hasattr(aliases, '__iter__'):
        aliases = set(aliases)
    else:
        aliases = set([aliases])
    return aliases

def with_replicas(aliases):
    """
    @with_replicas([alias,...])
    def func...

    Read from replicas despite pinning state.
    """
    aliases = _mash_aliases(aliases)
    # FIXME: test this.
    def make_wrapper(func):
        replicas = [unpinned_replica(alias) for alias in aliases]
        @wraps(func)
        def wrapper(*args, **kwargs):
            with contextlib.nested(*replicas):
                return func(*args, **kwargs)
        return wrapper
    return make_wrapper


class master(object):
    """
    with master("default"):
        ...

    Write to master despite (and without affecting) pinning state.

    """
    # TODO: make this optionally take a list of models for which to pin appropriately.
    def __init__(self, alias):
        self.alias = alias

    def __enter__(self):
        self.was_pinned = is_pinned(self.alias)
        pin(self.alias)

    def __exit__(self, type, value, tb):
        if not self.was_pinned:
            _unpin_one(self.alias)

        if any((type, value, tb)):
            raise type, value, tb

def with_masters(aliases):
    """
    @with_masters([alias,...])
    def func...

    Write to masters despite (and without affecting) pinning state.
    """
    aliases = _mash_aliases(aliases)
    # FIXME: test this.
    def make_wrapper(func):
        masters = [master(alias) for alias in aliases]
        @wraps(func)
        def wrapper(*args, **kwargs):
            with contextlib.nested(*masters):
                return func(*args, **kwargs)
        return wrapper
    return make_wrapper

# TODO: add logging to aid debugging client code.
def populate_replicas(masters, replicas_overrides, unmanaged_default=False):
    if not 'default' in masters and not unmanaged_default:
        raise PinDBConfigError("You must declare a default master")

    ret = {}
    for alias, master_values in masters.items():
        ret[alias] = master_values
        try:
            replica_overrides = replicas_overrides[alias]
        except KeyError:
            raise PinDBConfigError("No replica settings found for db set %s" % alias)
        for i, replica_override in enumerate(replica_overrides):
            replica_alias = _make_replica_alias(alias, i)
            replica_settings = master_values.copy()
            replica_settings.update(replica_override)
            replica_settings['TEST_MIRROR'] = alias
            ret[replica_alias] = replica_settings

    return ret

class DummyRouter(object):
    def db_for_read(self, model, **hints):
        return "default"

    def db_for_write(self, model, **hints):
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_syncdb(slef, db, model):
        return True

class PinDbRouterBase(object):
    def __init__(self):
        if (not hasattr(settings, 'MASTER_DATABASES') or
            not hasattr(settings, 'DATABASE_SETS')):
            raise PinDBConfigError("You must define MASTER_DATABASES and DATABASE_SETS settings.")

        # stash the # to chose from to reduce per-call overhead in the routing.
        for alias, master_values in settings.MASTER_DATABASES.items():
            _locals.DB_SET_SIZES[alias] = len(settings.DATABASE_SETS[alias]) - 1
            if _locals.DB_SET_SIZES[alias] == -1:
                warn("No replicas found for %s; using just the master" % alias)

        # defer master selection to a domain-specific router.
        delegates = getattr(settings, 'PINDB_DELEGATE_ROUTERS', [])
        if delegates:
            from django.db.utils import ConnectionRouter
            self.delegate = ConnectionRouter(delegates)
        else:
            warn("Unable to load delegate router from settings.PINDB_DELEGATE_ROUTERS; using default and its replicas")
            # or just always use default's set.
            self.delegate = DummyRouter()

    def db_for_read(self, model, **hints):
        master_alias = self.delegate.db_for_read(model, **hints)
        if master_alias is None:
            master_alias = "default"

        # allow anything unmanaged by the db set system to work unhindered.
        if not master_alias in settings.MASTER_DATABASES:
            return master_alias

        if is_pinned(master_alias):
            return master_alias
        return get_replica(master_alias)

    def db_for_write(self, model, **hints):
        master_alias = self.delegate.db_for_write(model, **hints)
        if master_alias is None:
            master_alias = "default"
        # allow anything unmanaged by the db set system to work unhindered.
        if not master_alias in settings.MASTER_DATABASES:
            return master_alias
        return self._for_write_with_policy(master_alias, model, **hints)

    def allow_relation(self, obj1, obj2, **hints):
        return self.delegate.allow_relation(obj1, obj2, **hints)

    def allow_syncdb(self, db, model):
        return self.delegate.allow_syncdb(db, model)

class StrictPinDBRouter(PinDbRouterBase): 
    def _for_write_with_policy(self, master_alias, model, **hints):
        if not is_pinned(master_alias):
            raise UnpinnedWriteException("Writes to %s aren't allowed because reads aren't pinned to it." % master_alias)
        return master_alias
    
class GreedyPinDBRouter(PinDbRouterBase): 
    def _for_write_with_policy(self, master_alias, model, **hints):
        if not is_pinned(master_alias):
            pin(master_alias)
        return master_alias
    
