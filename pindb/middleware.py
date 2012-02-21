from time import time

from django.conf import settings

from . import pin, get_newly_pinned, unpin_all


# The name of the cookie that directs a request's reads to the master DB
PINNING_COOKIE = getattr(settings, 'PINDB_PINNING_COOKIE',
                         'pindb_pinned_set')

# The number of seconds for which reads are directed to the master DB after a
# write
PINNING_SECONDS = int(getattr(settings, 'PINDB_PINNING_SECONDS', 15))

def _get_request_pins(cookie_value):
    ret = []
            
    now_time = time()
    pinned_untils = anyjson.loads(cookie_value)

    for pinned, until in pinned_untils:
        if not pinned in settings.MASTER_DATABASES:
            continue
        if now_time < until:
            ret.append((pinned, until))
    return ret

def _get_response_pins(request_pinned_until):
    pinned_until = request_pinned_until.copy()

    newly_pinned_set = get_newly_pinned()
    for pinned in newly_pinned_set:
        pinned_until[pinned] = time() + PINNING_SECONDS
    
    return pinned_until

class PinDBMiddleware(object):
    """Middleware to support the persisting pinning between requests after a write.

    Attaches a cookie to browser which has just written, causing subsequent
    DB reads (for some period of time, hopefully exceeding replication lag)
    to be handled by the master.

    When the cookie is detected on a request, related DBs are pinned.
    """
    def process_request(self, request):
        """Set the thread's pinning flag according to the presence of the
        incoming cookie."""

        # Make a clean slate
        unpin_all()

        request._pinned_until = {}

        if not PINNING_COOKIE in request.COOKIES:
            return

        for pinned, until in _get_request_pins(request.COOKIES[PINNING_COOKIE])
                    # keep track of existing end times for the return trip.
                    request._pinned_until[pinned] = until
                    pin(pinned, count_as_new=False)

    def process_response(self, request, response):
        pinned_until = _get_response_pins(request._pinned_until)

        to_persist = list(pinned_until.items())
        # don't set the cookie if there are no effective pins.
        if not to_persist:
            return response
    
        response.set_cookie(PINNING_COOKIE, 
            value=anyjson.dumps(to_persist),
            max_age=PINNING_SECONDS)

        return response
