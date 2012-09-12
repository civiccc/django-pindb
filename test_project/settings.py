from pindb import populate_replicas
from django.utils.datastructures import SortedDict


DEBUG = True

TEST_RUNNER = "test_project.test_runner.PinDbTestSuiteRunner"

ROOT_URLCONF = 'test_project.urls'

INSTALLED_APPS = (
    'pindb',
    'test_app',
)

MIDDLEWARE_CLASSES = (
    'pindb.middleware.PinDbMiddleware',
)

SECRET_KEY = "1e1909jd10d9joKMLAKXklmax"