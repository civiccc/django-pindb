from __future__ import absolute_import

from copy import deepcopy
import os, tempfile
from threading import local

from django import VERSION as dj_VERSION
from django.http import HttpRequest, HttpResponse
from django import db as dj_db
from django.db.backends.dummy.base import DatabaseWrapper as DummyDatabaseWrapper
from django.db.utils import ConnectionHandler, ConnectionRouter
from django.conf import settings
from django.core.management import call_command
# TransactionTestCase is used instead of TestCase
#  because TestCase holds db changes in a pending transaction,
#  which are not visible from the replica connection (even though)
#  it is TEST_MIRROR'd.

from django.test import TransactionTestCase 
from django.test.simple import DjangoTestSuiteRunner
from django.utils import importlib

import anyjson
from mock import patch
from override_settings import override_settings  # a backport from Django 1.4

from test_project.test_app.models import HamModel, EggModel, FrobModel

import pindb
from pindb import middleware
from pindb.exceptions import PinDbConfigError, UnpinnedWriteException

"""
Test writing without pinning
Test writing after pinning
Test writing after unpinning
Test unmanaged aliases (not in MASTER/_SETS) work

Test round-robin DB
Test delegate router used
test that TEST_MIRROR was effective.
test NUM_REPLICAS set correctly.
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

class InitTestCase(TransactionTestCase):
    def setUp(self):
        super(InitTestCase, self).setUp()
        pindb.DB_SET_SIZES = {'wark':1}

    def test_init_worked(self):
        self.assertEqual({"wark":1}, pindb.DB_SET_SIZES)
        pindb._init_state()
        self.assertEqual({}, pindb.DB_SET_SIZES)


# TODO: add coverage, COVERAGE_MODULE_EXCLUDES
class PinDbTestCase(TransactionTestCase):
    multi_db = True

    def _pre_setup(self):
        """Munge DB infrastructure before the superclass gets a chance to set up the DBs."""
        # clear all module state
        pindb._init_state()

        # patch up the db system to use the effective router settings (see override_settings)
        # we can't just reconstruct objects here because
        #   lots of places do from foo import baz, so they have
        #   a local reference to an object we can't replace.
        # so reach in and (gulp) mash the object's state.
        if dj_VERSION < (1, 4):
            for conn in dj_db.connections._connections.values():
                conn.close()
            dj_db.connections._connections = {}
        else:
            for conn in dj_db.connections.all():
                conn.close()
            dj_db.connections._connections = local()
        dj_db.connections.databases = settings.DATABASES

        def make_router(import_path):
            module_path, class_name = import_path.rsplit('.', 1)
            mod = importlib.import_module(module_path)
            return getattr(mod, class_name)()

        dj_db.router.routers = [
            make_router(import_path) for import_path in
            settings.DATABASE_ROUTERS]

        dj_db.connection = dj_db.connections[dj_db.DEFAULT_DB_ALIAS]
        dj_db.backend = dj_db.load_backend(dj_db.connection.settings_dict['ENGINE'])

        self.shim_runner = DjangoTestSuiteRunner()

        self.setup_databases()

        super(PinDbTestCase, self)._pre_setup()

    def _post_teardown(self):
        """Delete the databases after the superclass' method has closed the connections.

        We must do the DB deletion in post-teardown, because that's when the
        superclass closes its connection, which inadvertantly re-creates the
        sqlite file if we had previously tried to delete it (like in a plain
        old tearDown method).

        """
        super(PinDbTestCase, self)._post_teardown()
        pindb.unpin_all()
        self.teardown_databases(self.old_config)

    def setup_databases(self, **kwargs):
        self.old_config = self.shim_runner.setup_databases(**kwargs)

    def teardown_databases(self, old_config, **kwargs):
        self.shim_runner.teardown_databases(self.old_config)

    def _get_response_cookie(self, url):
        response = self.client.post(url)
        self.assertTrue(middleware.PINNING_COOKIE in response.cookies)
        return sorted(
                anyjson.loads(response.cookies[middleware.PINNING_COOKIE].value)
        )

def populate_databases(settings_dict):
    """Given a dict of various DB-related settings (DATABASES, MASTER_DATABASES, DATABASE_SETS), finalize all the settings into the DATABASES item of the dict.

    Also, turn the SQLite DBs we're using for testing into disk-based ones
    rather than memory-based ones, and give each a unique FS path, because
    Django buggily fails to come up with unique identifying tuples for multiple
    memory-based SQLites. Give each one a TEST_NAME, because otherwise,
    django.db.backends.sqlite3.creation reverts to ":memory:".

    """
    db_dir = tempfile.mkdtemp()
    if 'DATABASES' not in settings_dict:
        settings_dict['DATABASES'] = {}
    replicas =  pindb.populate_replicas(
        settings_dict['MASTER_DATABASES'],
        settings_dict['DATABASE_SETS']
    )

    settings_dict['DATABASES'].update(
        replicas
    )
    for alias, db in settings_dict['DATABASES'].items():
        db['NAME'] = db['TEST_NAME'] = os.path.join(db_dir, "test_%s" % alias)


misconfigured_settings = {
    'DATABASE_ROUTERS': ['pindb.StrictPinDbRouter'],
}  # because MASTER_DATAABASES and DATABASE_SETS is required.

@override_settings(**misconfigured_settings)
class MisconfiguredTest(TransactionTestCase):
    multi_db = True  # Necessary? Tests pass without.

    def test_router_catches_misconfiguration(self):
        self.assertRaises(PinDbConfigError, ConnectionRouter, settings.DATABASE_ROUTERS)

    def test_populate_replicas_catches_misconfiguration(self):
        no_default = {
            'DATABASE_ROUTERS': ['pindb.StrictPinDbRouter'],
            'MASTER_DATABASES': dict([
                ('ham', {
                    'NAME': ':memory:',
                    'ENGINE': 'django.db.backends.sqlite3',
                }),
                ('egg', {
                    'NAME': ':memory:',
                    'ENGINE': 'django.db.backends.sqlite3',
                })
            ]),
            'DATABASE_SETS': {
                'ham': [],
                # This normally would have overrides, but lots of sqlites in
                # memory are happy together:
                'egg': [{}, {}]
            },
            'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
        }

        self.assertRaises(PinDbConfigError,
            pindb.populate_replicas,
            no_default['MASTER_DATABASES'],
            {}
        )

        default_ok = no_default.copy()  # Note: not deep, but doesn't matter
        default_ok['MASTER_DATABASES']['default'] = default_ok['MASTER_DATABASES']['ham']
        del default_ok['MASTER_DATABASES']['ham']
        try:
            pindb.populate_replicas(
                default_ok['MASTER_DATABASES'],
                 {'default': [], 'egg': []})
        except PinDbConfigError:
            self.fail("Expected default to be acceptable config.")


no_delegate_router_settings = {
    'DATABASE_ROUTERS': ['pindb.StrictPinDbRouter'],
    'MASTER_DATABASES': {
        'default':  {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        },
        'egg': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        }
    },
    'DATABASE_SETS': {
        # This normally would have overrides, but lots of sqlites in memory are
        # happy together:
        'default': [{}, {}],
        'egg': [] #no replicas
    },
    'PINDB_DELEGATE_ROUTERS': None
}
populate_databases(no_delegate_router_settings)

@override_settings(**no_delegate_router_settings)
class NoDelegateTest(PinDbTestCase):
    def test_internals(self):
        self.assertEqual(pindb.DB_SET_SIZES['default'], 1)
        self.assertEqual(pindb.DB_SET_SIZES['egg'], -1)

    def test_pinning(self):
        # pinning is reflected in is_pinned
        for master in settings.MASTER_DATABASES:
            self.assertFalse(pindb.is_pinned(master))
            pindb.pin(master)
            self.assertTrue(pindb.is_pinned(master))
        # unpin_all resets the state.
        pindb.unpin_all()
        for master in settings.MASTER_DATABASES:
            self.assertFalse(pindb.is_pinned(master))

        pindb.pin("default")
        # pinning state is available as a set.
        pinned1 = pindb.get_pinned()
        self.assertTrue('default' in pinned1)
        # unpinning doesn't affect the previous set.
        pindb.unpin_all()
        pinned2 = pindb.get_pinned()
        self.assertTrue('default' in pinned1)
        self.assertFalse('default' in pinned2)

        # we can keep track of new pins vs. carried-over pins
        #  i.e. pins that stick for the replication lag period.
        self.assertEqual(0, len(pindb.get_newly_pinned()))
        pindb.pin("default")
        self.assertEqual(1, len(pindb.get_newly_pinned()))
        pindb.unpin_all()
        self.assertEqual(0, len(pindb.get_newly_pinned()))
        pindb.pin("default", count_as_new=False)
        self.assertEqual(0, len(pindb.get_newly_pinned()))
        # it counts as pinned even if we don't count it as new.
        self.assertEqual(1, len(pindb.get_pinned()))

    @patch("pindb.randint")
    def test_unpinned_replica(self, mock_randint):
        pindb.pin("default")
        self.assertEqual(
            dj_db.router.db_for_read(HamModel), "default"
        )
        with pindb.unpinned_replica("default"):
            mock_randint.return_value = 1
            self.assertEqual(
                dj_db.router.db_for_read(HamModel), "default-1"
            )

        with self.assertRaises(ValueError):
            with pindb.unpinned_replica("default"):
                raise ValueError

        @pindb.with_replicas("default")
        def to_replicas():
            mock_randint.return_value = 1
            self.assertEqual(
                dj_db.router.db_for_read(HamModel), "default-1"
            )
        to_replicas()

        @pindb.with_replicas(["default"])
        def to_replicas_list():
            mock_randint.return_value = 2
            self.assertEqual(
                dj_db.router.db_for_read(HamModel), "default-1"
            )
        to_replicas_list()

    @patch("pindb.randint")
    def test_with_master(self, mock_randint):
        mock_randint.return_value = 0
        self.assertEqual(
            dj_db.router.db_for_read(HamModel), "default-0"
        )
        with pindb.master("default"):
            self.assertEqual(
                dj_db.router.db_for_read(HamModel), "default"
            )
        self.assertEqual(
            dj_db.router.db_for_read(HamModel), "default-0"
        )

        @pindb.with_masters(["default"])
        def to_master():
            mock_randint.return_value = 2
            self.assertEqual(
                dj_db.router.db_for_read(HamModel), "default"
            )

    @patch("pindb.randint")
    def test_get_replica(self, mock_randint):
        mock_randint.return_value = 0
        self.assertEqual(pindb.get_replica("default"), "default-0")
        mock_randint.return_value = 1
        self.assertEqual(pindb.get_replica("default"), "default-0")

        # gets the master if there are no replicas
        self.assertEqual(pindb.get_replica("egg"), "egg")
        self.assertEqual(pindb.get_replica("frob"), "frob")

    def test_router(self):
        self.assertTrue(
            dj_db.router.db_for_read(HamModel) in ["default-0", "default-1"]
        )
        self.assertRaises(
            UnpinnedWriteException,
            dj_db.router.db_for_write,
            HamModel
        )
        self.assertTrue(
            dj_db.router.db_for_read(EggModel) in ["default-0", "default-1"]
        )
        self.assertRaises(
            UnpinnedWriteException,
            dj_db.router.db_for_write,
            EggModel
        )
        pindb.pin("default")
        try:
            self.assertEqual('default', dj_db.router.db_for_write(HamModel))
        except UnpinnedWriteException:
            self.fail("Expected to be able to write after pinning.")

        ham1 = HamModel.objects.create()
        ham2 = HamModel.objects.create()
        # If no delegate router is given, all DB goes to default.
        egg = EggModel.objects.create()
        self.assertTrue(dj_db.router.allow_relation(ham1, ham2))
        self.assertTrue(dj_db.router.allow_syncdb("default", HamModel))
        self.assertTrue(dj_db.router.allow_syncdb("default", EggModel))


delegate_strict_router_settings = {
    'DATABASE_ROUTERS': ['pindb.StrictPinDbRouter'],
    'DATABASES': {
        'frob': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        }
    },
    'MASTER_DATABASES': {
        'default': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        },
        'egg': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        }
    },
    'DATABASE_SETS': {
        # This normally would have overrides, but lots of sqlites in memory are
        # happy together:
        'default': [],
        'egg': [{}, {}]
    },
    'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
}
populate_databases(delegate_strict_router_settings)

@override_settings(**delegate_strict_router_settings)
class FullyConfiguredStrictTest(PinDbTestCase):
    def test_internals(self):
        self.assertEqual(pindb.DB_SET_SIZES['default'], -1)
        self.assertEqual(pindb.DB_SET_SIZES['egg'], 1)

    def test_pinning(self):
        # pinning is reflected in is_pinned
        for master in settings.MASTER_DATABASES:
            self.assertFalse(pindb.is_pinned(master))
            pindb.pin(master)
            self.assertTrue(pindb.is_pinned(master))
        # unpin_all resets the state.
        pindb.unpin_all()
        for master in settings.MASTER_DATABASES:
            self.assertFalse(pindb.is_pinned(master))

        pindb.pin("default")
        # pinning state is available as a set.
        pinned1 = pindb.get_pinned()
        self.assertTrue('default' in pinned1)
        # unpinning doesn't affect the previous set.
        pindb.unpin_all()
        pinned2 = pindb.get_pinned()
        self.assertTrue('default' in pinned1)
        self.assertFalse('default' in pinned2)

        # we can keep track of new pins vs. carried-over pins
        #  i.e. pins that stick for the replication lag period.
        self.assertEqual(0, len(pindb.get_newly_pinned()))
        pindb.pin("default")
        self.assertEqual(1, len(pindb.get_newly_pinned()))
        pindb.unpin_all()
        self.assertEqual(0, len(pindb.get_newly_pinned()))
        pindb.pin("default", count_as_new=False)
        self.assertEqual(0, len(pindb.get_newly_pinned()))
        # it counts as pinned even if we don't count it as new.
        self.assertEqual(1, len(pindb.get_pinned()))

    @patch("pindb.randint")
    def test_get_replica(self, mock_randint):
        mock_randint.return_value = 0
        self.assertEqual(pindb.get_replica("default"), "default")
        mock_randint.return_value = 1
        self.assertEqual(pindb.get_replica("default"), "default")

        # gets the master if there are no replicas
        mock_randint.return_value = 0
        self.assertEqual(pindb.get_replica("egg"), "egg-0")
        mock_randint.return_value = 1
        self.assertEqual(pindb.get_replica("egg"), "egg-0")

        # nonexistent or unmanaged DATABASES should return the alias
        self.assertEqual(pindb.get_replica("frob"), "frob")
        self.assertEqual(pindb.get_replica("nope"), "nope")

    def test_router(self):
        self.assertEqual(
            dj_db.router.db_for_read(HamModel), "default"
        )
        self.assertRaises(
            UnpinnedWriteException,
            dj_db.router.db_for_write,
            HamModel
        )
        self.assertTrue(
            dj_db.router.db_for_read(EggModel) in ["egg-0", "egg-1"]
        )
        self.assertRaises(
            UnpinnedWriteException,
            dj_db.router.db_for_write,
            EggModel
        )
        pindb.pin("default")
        try:
            self.assertEqual('default', dj_db.router.db_for_write(HamModel))
        except UnpinnedWriteException:
            self.fail("Expected to be able to write after pinning.")

        ham1 = HamModel.objects.create()
        ham2 = HamModel.objects.create()
        # pinning a different DB doesn't allow writes on others.
        self.assertRaises(
            UnpinnedWriteException,
            dj_db.router.db_for_write,
            EggModel
        )

        pindb.pin("egg")
        try:
            egg = EggModel.objects.create()
        except UnpinnedWriteException:
            self.fail("Expected to be able to write after pinning.")

        self.assertTrue(dj_db.router.allow_relation(ham1, ham2))
        self.assertTrue(dj_db.router.allow_syncdb("default", HamModel))
        self.assertTrue(dj_db.router.allow_syncdb("default", EggModel))

        self.assertEqual(dj_db.router.db_for_read(FrobModel), "default")


delegate_greedy_router_settings = {
    'DATABASE_ROUTERS': ['pindb.GreedyPinDbRouter'],
    'DATABASES': {
        'frob': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        }
    },
    'MASTER_DATABASES': {
        'default': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        },
        'egg': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        }
    },
    'DATABASE_SETS': {
        # This normally would have overrides, but lots of sqlites in memory are
        # happy together:
        'default': [],
        'egg': [{}, {}]
    },
    'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
}
greedy_middleware_settings = deepcopy(delegate_greedy_router_settings)
populate_databases(delegate_greedy_router_settings)
populate_databases(greedy_middleware_settings)  # for GreedyMiddlewareTest

@override_settings(**delegate_greedy_router_settings)
class FullyConfiguredGreedyTest(PinDbTestCase):
    """Tests for a ``GreedyPinDbRouter``, complete with delegate router"""

    def test_router(self):
        # Ham should go in the default DB:
        self.assertEqual(dj_db.router.db_for_read(HamModel), "default")
        # Reading one shouldn't cause it to pin:
        self.assertEqual(pindb.is_pinned("default"), False)
        # The delegate router has no opinion on Hams, so they should write to
        # the default DB:
        self.assertEqual(dj_db.router.db_for_write(HamModel), "default")
        # The above should cause it to pin (even though default has no slaves
        # in this case):
        self.assertEqual(pindb.is_pinned("default"), True)

        # Eggs are stored in a replicated DB set, so we should read from a replica:
        self.assertTrue(dj_db.router.db_for_read(EggModel) in ["egg-0", "egg-1"])
        # Reading shouldn't pin:
        self.assertEqual(pindb.is_pinned("egg"), False)
        # Writes should go to master:
        self.assertEqual(dj_db.router.db_for_write(EggModel), "egg")
        # And should cause a pin:
        self.assertEqual(pindb.is_pinned("egg"), True)
        pindb.unpin_all()

        # Making a Ham should pin to the master of the default set...
        ham1 = HamModel.objects.create()
        self.assertEqual(pindb.is_pinned("default"), True)
        # ...but not the egg set:
        self.assertEqual(pindb.is_pinned("egg"), False)

        # Making an Egg should pin the egg set as well:
        egg1 = EggModel.objects.create()
        self.assertEqual(pindb.is_pinned("egg"), True)
        pindb.unpin_all()

        ham1a = HamModel.objects.get(pk=ham1.pk)
        egg1a = EggModel.objects.get(pk=egg1.pk)  # no longer bewm

    def test_new_pins_persist(self):
        """If a greedy router scoops up a new pinning, make sure it counts as new.

        If it doesn't middleware and such will fail to persist it.

        """
        # Unprovoked by a write, it returns a replica:
        self.assertTrue(dj_db.router.db_for_read(EggModel) in ["egg-0", "egg-1"])
        # Then we write:
        self.assertEqual(dj_db.router.db_for_write(EggModel), "egg")
        # And then it should be in the newly pinned set:
        self.assertTrue(pindb.is_newly_pinned("egg"))

    def test_misdirected_save(self):
        """Test that saves route to the right DB after a fetch.

        At some point in production, we had an error that looked like fetching
        a model instance (from a slave) and then saving it failed, because it
        was trying to to write to the slave. It sure doesn't seem to to that
        [anymore].

        """
        # Set up the pre-existing model instance:
        egg = EggModel.objects.create()
        egg_id = egg.id
        pindb.unpin_all()

        egg = EggModel.objects.get(id=egg_id)
        self.assertEqual(dj_db.router.db_for_write(EggModel), "egg")


@override_settings(**greedy_middleware_settings)
class GreedyMiddlewareTest(PinDbTestCase):
    def test_read(self):
        response = self.client.post('/test_app/read/')
        self.assertFalse(middleware.PINNING_COOKIE in response.cookies)

    @patch('pindb.middleware.time')
    def test_write(self, mock_time):
        mock_time.return_value = 1
        cookie = self._get_response_cookie('/test_app/write/')
        self.assertEqual(cookie, [["default", 1 + middleware.PINNING_SECONDS]])
        mock_time.return_value = 2
        cookie = self._get_response_cookie('/test_app/write/')
        self.assertEqual(cookie, [["default", 2 + middleware.PINNING_SECONDS]])

    @patch('pindb.middleware.time')
    def test_write_with_existing(self, mock_time):
        mock_time.return_value = 1
        cookie = self._get_response_cookie('/test_app/write/')
        self.assertEqual(cookie, [["default", 1 + middleware.PINNING_SECONDS]])
        mock_time.return_value = 2

        cookie = self._get_response_cookie('/test_app/create_one_pin/')
        self.assertEqual(cookie, [
            ["default", 1 + middleware.PINNING_SECONDS],
            ["egg", 2 + middleware.PINNING_SECONDS],
        ])

    def test_bad_cookie(self):
        self.assertEquals(middleware._get_request_pins('bad thing'), [])

disabled_settings = {
    'PINDB_ENABLED': False,
    'DATABASE_ROUTERS': ['pindb.GreedyPinDbRouter'],
    'MASTER_DATABASES': {
        'default':  {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        },
        'egg': {
            'NAME': ':memory:',
            'ENGINE': 'django.db.backends.sqlite3',
        }
    },
    'DATABASE_SETS': {
        # This normally would have overrides, but lots of sqlites in memory are
        # happy together:
        'default': [{}, {}],
        'egg': [] #no replicas
    },
    'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
}
populate_databases(disabled_settings)

@override_settings(**disabled_settings)
class DisabledTest(PinDbTestCase):
    def test_read(self):
        response = self.client.post('/test_app/read/')
        self.assertFalse(middleware.PINNING_COOKIE in response.cookies)
    
    def test_write(self):
        response = self.client.post('/test_app/write/')
        self.assertFalse(middleware.PINNING_COOKIE in response.cookies)

    def test_router(self):
        # Ham should go in the default DB:
        self.assertEqual(dj_db.router.db_for_read(HamModel), "default")
        # Reading one shouldn't cause it to pin:
        self.assertEqual(pindb.is_pinned("default"), False)
        # The delegate router has no opinion on Hams, so they should write to
        # the default DB:
        self.assertEqual(dj_db.router.db_for_write(HamModel), "default")
        # The above should *not* cause it to pin since we're disabled.
        self.assertEqual(pindb.is_pinned("default"), False)

        # Eggs are stored in a replicated DB set, but since we're disabled, 
        #   we should still get the master.
        self.assertEqual(dj_db.router.db_for_read(EggModel), "egg")
        # Reading shouldn't pin:
        self.assertEqual(pindb.is_pinned("egg"), False)
        # Writes should go to master:
        self.assertEqual(dj_db.router.db_for_write(EggModel), "egg")
        # but still should *not* cause it to pin since we're disabled.
        self.assertEqual(pindb.is_pinned("egg"), False)
