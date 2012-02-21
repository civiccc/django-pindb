try:
    from celery.signals import task_prerun
except ImportError:
    task_prerun = None

from . import unpin_all

# It's assumed for now that different tasks don't need to be pinned.
#  We can discover the use cases as needed.
if task_prerun:
    unpin_all()
