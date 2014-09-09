'''
    "The two most important days in your life are the day you are born and the
    day you find out why."

     –Mark Twain
'''


# This is a custom test runner as per the instructions for django test runners
# https://docs.djangoproject.com/en/dev/topics/testing/advanced/#other-testing-frameworks

from django.conf import settings
from djcelery.contrib.test_runner import CeleryTestSuiteRunner

USAGE = """\
Custom test runner to allow for testing celery delayed and async tasts as well
as AMQP broker tests.
"""


def _set_test_overrides():
    settings.DPN_BROKER_URL = "memory://"


class DPNodeTestSuiteRunner(CeleryTestSuiteRunner):
    def setup_test_environment(self, **kwargs):
        super(DPNodeTestSuiteRunner, self).setup_test_environment(**kwargs)
        _set_test_overrides()