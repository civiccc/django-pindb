from pindb import populate_replicas
from django.utils.datastructures import SortedDict


DEBUG = True

MASTER_DATABASES = SortedDict([
    ('ham', {
        'NAME': ':memory:',
        'ENGINE': 'django.db.backends.sqlite3',
    }),
    ('egg', {
        'NAME': ':memory:',
        'ENGINE': 'django.db.backends.sqlite3',        
    })
])

DATABASE_SETS = {
    'ham': [],
    'eggs': [{}, {}] # normally would have overrides, but lots of sqlites in memory are happy together.
}

DATABASES = populate_replicas(MASTER_DATABASES, DATABASE_SETS)

DATABASE_ROUNTER_DELEGATE = 'test_project.router.FrobRouter'

ROOT_URLCONF = 'test_project.urls'

INSTALLED_APPS = (
    'test_app',
)