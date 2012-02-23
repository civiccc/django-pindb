import tempfile, os
from mock import patch

from django.http import HttpRequest, HttpResponse
from django import db as dj_db
from django.db.backends.dummy.base import DatabaseWrapper as DummyDatabaseWrapper 
from django.db.utils import ConnectionHandler, ConnectionRouter
from django.conf import settings
from django.core.management import call_command
from django.test import TestCase
from django.test.simple import DjangoTestSuiteRunner
from django.utils import importlib

# a backport from django 1.4
from override_settings import override_settings

from test_project.test_app.models import HamModel, EggModel, FrobModel

import pindb
from pindb.exceptions import PinDBConfigError, UnpinnedWriteException


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


# TODO: add coverage, COVERAGE_MODULE_EXCLUDES
class PinDBTestCase(TestCase):
    multi_db = True

    def _fixture_setup(self):
        # nope, we have no fixtures, and don't want to 
        #  insist on a db existing at this point because setUp will make them.
        pass 

    def _post_teardown(self):
        """ Performs any post-test things. This includes:

            * Putting back the original ROOT_URLCONF if it was changed.
            * Force closing the connection, so that the next test gets
              a clean cursor.
        """
        self._fixture_teardown()
        self._urlconf_teardown()
        # Some DB cursors include SQL statements as part of cursor
        # creation. If you have a test that does rollback, the effect
        # of these statements is lost, which can effect the operation
        # of tests (e.g., losing a timezone setting causing objects to
        # be created with the wrong time).
        # To make sure this doesn't happen, get a clean connection at the
        # start of every test.
        for connection in dj_db.connections.all():
            connection.close()

    def _fixture_teardown(self):
        # patching to clear out models created
        for db in dj_db.connections:
            # skip dummy backends, which only show up when 
            #  we haven't config'd, and which can't be flushed.
            if isinstance(dj_db.connections[db], DummyDatabaseWrapper):
                continue
            call_command('flush', verbosity=0, interactive=False, database=db)

    def setUp(self):
        # clear all module state
        pindb._init_state()

        # patch up the db system to use the effective router settings (see override_settings)
        # we can't just reconstruct objects here because 
        #   lots of places do from foo import baz, so they have
        #   a local reference to an object we can't replace.
        # so reach in and (gulp) mash the object's state.
        dj_db.connections.databases = settings.DATABASES
        for conn in dj_db.connections._connections.values():
            conn.close()
        dj_db.connections._connections = {}

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
    
    def tearDown(self):
        pindb.unpin_all()
        self.teardown_databases(self.old_config)

    def setup_databases(self, **kwargs):
        self.old_config = self.shim_runner.setup_databases(**kwargs)

    def teardown_databases(self, old_config, **kwargs):
        self.shim_runner.teardown_databases(self.old_config)

def populate_databases(settings_dict):
    # work around a bug in django - it doesn't properly create multiple
    #  in-memory sqlite dbs  So we'll go disk-based.
    db_dir = tempfile.mkdtemp()
    if 'DATABASES' not in settings_dict:
        settings_dict['DATABASES'] = {}
    settings_dict['DATABASES'].update(
        pindb.populate_replicas(
            settings_dict['MASTER_DATABASES'], 
            settings_dict['DATABASE_SETS']
        )
    )
    for alias, db in settings_dict['DATABASES'].items():
        db['NAME'] = os.path.join(db_dir, "test_%s" % alias)

misconfigured_settings = {
    'DATABASE_ROUTERS': ['pindb.StrictPinDBRouter'],
} #because MASTER_DATAABASES and DATABASE_SETS is required.

@override_settings(**misconfigured_settings)
class MisconfiguredTest(PinDBTestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass

    def test_router_catches_misconfiguration(self):                
        self.assertRaises(PinDBConfigError, ConnectionRouter, settings.DATABASE_ROUTERS)
    
    def test_populate_replicas_catches_misconfiguration(self):
        no_default = {
            'DATABASE_ROUTERS': ['pindb.StrictPinDBRouter'],
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
                'egg': [{}, {}] # normally would have overrides, but lots of sqlites in memory are happy together.
            },
            'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
        }

        self.assertRaises(PinDBConfigError,
            pindb.populate_replicas,
            no_default['MASTER_DATABASES'], 
            {}
        )

        default_ok = no_default.copy()
        default_ok['MASTER_DATABASES']['default'] = default_ok['MASTER_DATABASES']['ham']
        del default_ok['MASTER_DATABASES']['ham']
        try:
            pindb.populate_replicas(
                default_ok['MASTER_DATABASES'], 
                 {'default': [], 'egg': []})
        except PinDBConfigError:
            self.fail("Expected default to be acceptable config.")

no_delegate_router_settings = {
    'DATABASE_ROUTERS': ['pindb.StrictPinDBRouter'],
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
        'default': [{}, {}], # normally would have overrides, but lots of sqlites in memory are happy together.
        'egg': [] #no replicas
    },
    'PINDB_DELEGATE_ROUTERS': None
}
populate_databases(no_delegate_router_settings)

@override_settings(**no_delegate_router_settings)
class NoDelegateTest(PinDBTestCase):
    def test_internals(self):
        self.assertEqual(pindb._locals.DB_SET_SIZES['default'], 1)
        self.assertEqual(pindb._locals.DB_SET_SIZES['egg'], -1)

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

    @patch("pindb.randint")
    def test_get_replica(self, mock_randint):
        mock_randint.return_value = 0
        self.assertEqual(pindb.get_replica("default"), "default-0")
        mock_randint.return_value = 1
        self.assertEqual(pindb.get_replica("default"), "default-1")

        # gets the master if there are no replicas
        self.assertEqual(pindb.get_replica("egg"), "egg")
        self.assertRaises(KeyError, pindb.get_replica, "frob")

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
    'DATABASE_ROUTERS': ['pindb.StrictPinDBRouter'],
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
        'default': [], # normally would have overrides, but lots of sqlites in memory are happy together.
        'egg': [{}, {}]
    },
    'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
}
populate_databases(delegate_strict_router_settings)

@override_settings(**delegate_strict_router_settings)
class FullyConfiguredStrictTest(PinDBTestCase):
    def test_internals(self):
        self.assertEqual(pindb._locals.DB_SET_SIZES['default'], -1)
        self.assertEqual(pindb._locals.DB_SET_SIZES['egg'], 1)

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
        self.assertEqual(pindb.get_replica("egg"), "egg-1")

        # nonexistent or unmanaged DATABASES should throw keyerror.
        self.assertRaises(KeyError, pindb.get_replica, "frob")
        self.assertRaises(KeyError, pindb.get_replica, "nope")

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
    'DATABASE_ROUTERS': ['pindb.GreedyPinDBRouter'],
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
        'default': [], # normally would have overrides, but lots of sqlites in memory are happy together.
        'egg': [{}, {}]
    },
    'PINDB_DELEGATE_ROUTERS': ['test_project.router.HamAndEggRouter']
}

populate_databases(delegate_greedy_router_settings)

@override_settings(**delegate_greedy_router_settings)
class FullyConfiguredGreedyTest(PinDBTestCase):
    def test_router(self):
        self.assertEqual(
            dj_db.router.db_for_read(HamModel), "default"
        )
        self.assertEqual(pindb.is_pinned("default"), False)
        self.assertEqual(
            dj_db.router.db_for_write(HamModel), "default"
        )
        self.assertEqual(pindb.is_pinned("default"), True)

        self.assertTrue(
            dj_db.router.db_for_read(EggModel) in ["egg-0", "egg-1"]
        )
        self.assertEqual(pindb.is_pinned("egg"), False)
        self.assertEqual(
            dj_db.router.db_for_write(EggModel), "egg"
        )
        self.assertEqual(pindb.is_pinned("egg"), True)

        pindb.unpin_all()

        ham1 = HamModel.objects.create()
        self.assertEqual(pindb.is_pinned("default"), True)
        self.assertEqual(pindb.is_pinned("egg"), False)
