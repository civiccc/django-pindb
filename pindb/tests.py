from django.http import HttpRequest, HttpResponse
from django.test import TestCase

from multidb import (DEFAULT_DB_ALIAS, MasterSlaveRouter,
    PinningMasterSlaveRouter, get_slave)
from multidb.middleware import (PINNING_COOKIE, PINNING_SECONDS,
    PinningRouterMiddleware)
from multidb.pinning import (this_thread_is_pinned,
    pin_this_thread, unpin_this_thread, use_master, db_write)


"""
Test writing without pinning
Test writing after unpinning
Test round-robin DB
Test delegate router used
Test web pinning context:
    test no cookie case
    test cookie with bad pin name (changed alias, user hacking)
    timeout
    persistence of different timestamps
    test wipes out dirty pins on request start 
Test task context:
    test wipes out dirty pins on request start 
    test write without pinning
    test writing after pinning
"""