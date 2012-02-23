from django.conf.urls.defaults import *
from . import views

urlpatterns = patterns('',
    # fixme: {% url %} instead of hard-code in tests.
    url(r"read", views.read, name='read'),
    url(r"write", views.write, name='write'),
    url(r"create_no_pins", views.create_no_pins, name='create_no_pins'),
    url(r"create_one_pin", views.create_one_pin, name='create_one_pin'),
)
