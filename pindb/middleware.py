from math import ceil
from time import time

from django.conf import settings

import anyjson

from . import pin, get_newly_pinned, unpin_all


# The name of the cookie that directs a request's reads to the master DB
PINNING_COOKIE = getattr(settings, 'PINDB_PINNING_COOKIE', 'pindb_pinned_set')

# The number of seconds for which reads are directed to the master DB after a
# write
PINNING_SECONDS = int(getattr(settings, 'PINDB_PINNING_SECONDS', 15))

def _get_request_pins(cookie_value):
    """Extract the persistent pinnings from a cookie.

    Return an iterable of (DB alias, time pinned until) tuples. Any expired
    pinnings are omitted.

    """
    ret = []

    now_time = time()
    try:
        pinned_untils = anyjson.loads(cookie_value)
    except ValueError:
        # If the cookie was corrupted, revert to not pinning anything:
        # TODO: Maybe add some logging.
        pinned_untils = []

    for alias, until in pinned_untils:
        if not alias in settings.MASTER_DATABASES:
            continue
        if now_time < until:
            ret.append((alias, until))
    return ret

def _get_response_pins(request_pinned_until):
    """Return the union of the preexisting pinned set--socked away on the request--with any newly set pins."""
    pinned_until = request_pinned_until.copy()

    # Update (a copy of) the previous persistent pinned set with any new pinnings:
    new_expiration = int(ceil(time() + PINNING_SECONDS))
    for alias in get_newly_pinned():
        pinned_until[alias] = new_expiration

    return pinned_until

class PinDbMiddleware(object):
    """Middleware to support the persisting pinning between requests after a write.

    Attaches a cookie to browser which has just written, causing subsequent
    DB reads (for some period of time, hopefully exceeding replication lag)
    to be handled by the master.

    When the cookie is detected on a request, related DBs are pinned.

    """
    def process_request(self, request):
        """Pin DB sets according to data in an incoming cookie."""
        # Make a clean slate. This is also necessary to ensure the threadlocal
        # attrs of our locals() object exist.
        unpin_all()

        request._pinned_until = {}

        if not PINNING_COOKIE in request.COOKIES:
            return

        for alias, until in _get_request_pins(request.COOKIES[PINNING_COOKIE]):
            # Keep track of existing end times for the return trip.
            request._pinned_until[alias] = until
            pin(alias, count_as_new=False)

    def process_response(self, request, response):
        """Set outgoing cookie to persist preexisting and new pinnings."""
        pinned_until = _get_response_pins(request._pinned_until)

        to_persist = list(pinned_until.items())
        # Don't set the cookie if there are no effective pins.
        if to_persist:
            # TODO: Use Django 1.4's signed cookies.
            response.set_cookie(PINNING_COOKIE,
                value=anyjson.dumps(to_persist),
                max_age=PINNING_SECONDS)

        return response
