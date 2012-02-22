from pindb import populate_replicas
from django.utils.datastructures import SortedDict


DEBUG = True

TEST_RUNNER = "test_project.test_runner.PinDBTestSuiteRunner"

ROOT_URLCONF = 'test_project.urls'

INSTALLED_APPS = (
    'pindb',
    'test_app',
)