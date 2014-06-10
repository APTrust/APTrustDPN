# import all tasks here so celery can register them
from .outbound import *
from .inbound import *
from .registry import *

__author__ = 'swt8w'