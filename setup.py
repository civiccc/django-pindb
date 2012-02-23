#!/usr/bin/env python

import os
import sys
import codecs

try:
    from setuptools import setup, Command, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, Command, find_packages  # noqa
from distutils.command.install import INSTALL_SCHEMES

src_dir = "pindb"

class RunTests(Command):
    description = "Run the django test suite from the tests dir."
    user_options = []
    extra_env = {}
    extra_args = []

    def run(self):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)

        this_dir = os.getcwd()
        testproj_dir = os.path.join(this_dir, "test_project")
        os.chdir(testproj_dir)
        sys.path.append(testproj_dir)
        from django.core.management import execute_manager
        os.environ["DJANGO_SETTINGS_MODULE"] = os.environ.get(
                        "DJANGO_SETTINGS_MODULE", "settings")
        settings_file = os.environ["DJANGO_SETTINGS_MODULE"]
        settings_mod = __import__(settings_file, {}, {}, [''])
        prev_argv = list(sys.argv)
        try:
            sys.argv = [__file__, "test"] + self.extra_args
            execute_manager(settings_mod, argv=sys.argv)
        finally:
            sys.argv = prev_argv

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


if os.path.exists("README.rst"):
    long_description = codecs.open("README", "r", "utf-8").read()
else:
    long_description = "See https://github.com/votizen/django-pindb"

setup(
    name = 'django-pindb',
    packages=find_packages(),
    version='0.1.5', # remember to change __init__
    description = 'Manages master/replica pinning for django',
    long_description = long_description,
    url = 'https://github.com/votizen/django-pindb',
    author = 'Jeremy Dunck',
    author_email = 'jdunck@votizen.com',
    maintainer = 'Jeremy Dunck',
    maintainer_email = 'jdunck@votizen.com',
    keywords = ['django', 'multidb', 'router'],
    license = 'MIT',
    tests_require=['django>=1.2.0', 'django-override-settings>=1.2', 'mock>=0.7.2'],    
    cmdclass={
        "test": RunTests,
    },
    include_package_data=True,
    platforms=["any"],
    classifiers=[
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
