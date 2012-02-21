from pindb import populate_replicas
from django.utils.datastructures import SortedDict

MASTER_DATABASES = SortedDict([
    ('frob', {
        'NAME': ':memory:',
        'ENGINE': 'django.db.backends.sqlite3',
    })
])

DATABASE_SETS = {
    'frob': [{}]
}

DATABASES = populate_replicas(MASTER_DATABASES, DATABASE_SETS)
