from django.http import HttpResponse
from .models import HamModel, EggModel
import pindb

def create_no_pins(request):
    with pindb.master("default"):
        HamModel.objects.create()
    return HttpResponse("create no pins")

def create_one_pin(request):
    with pindb.master("default"):
        HamModel.objects.create()
    EggModel.objects.create()
    return HttpResponse("create one pin")

def write(request):
    HamModel.objects.create()
    return HttpResponse("write")

def read(request):
    HamModel.objects.all()
    return HttpResponse("read")
