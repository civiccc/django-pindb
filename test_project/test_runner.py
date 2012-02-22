from django.test.simple import DjangoTestSuiteRunner

class PinDBTestSuiteRunner(DjangoTestSuiteRunner):
    # Hacking out db creation here so the test cases
    #  can handle it themselves.
    def setup_databases(self, **kwargs):
        pass

    def teardown_databases(self, old_config, **kwargs):
        pass
