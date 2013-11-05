# Set a reasonable Project Path setting so I dont' have to use hard coded paths.
import os
PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))

DEBUG = False # Always make False by default.
TEMPLATE_DEBUG = DEBUG

# CELERY WORKER CONFIG
BROKER_URL = 'amqp://guest:guest@localhost:5672/'

ADMINS = (
# ('Your Name', 'your_email@example.com'),
)

MANAGERS = ADMINS

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
        'NAME': '', # Or path to database file if using sqlite3.
        'USER': '', # Not used with sqlite3.
        'PASSWORD': '', # Not used with sqlite3.
        'HOST': '', # Set to empty string for localhost. Not used with sqlite3.
        'PORT': '', # Set to empty string for default. Not used with sqlite3.
    }
}

# Make this unique, and don't share it with anybody.
# Useful tool for this at http://www.miniwebtool.com/django-secret-key-generator/
SECRET_KEY = ''

# Hosts/domain names that are valid for this site; required if DEBUG is False
# See https://docs.djangoproject.com/en/1.4/ref/settings/#allowed-hosts
ALLOWED_HOSTS = []

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'America/New_York'

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/home/media/media.lawrence.com/media/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://media.lawrence.com/media/", "http://example.com/media/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/home/media/media.lawrence.com/static/"
STATIC_ROOT = ''

# URL prefix for static files.
# Example: "http://media.lawrence.com/static/"
STATIC_URL = '/assets/'

# Additional locations of static files
STATICFILES_DIRS = (
# Put strings here, like "/home/html/static" or "C:/www/django/static".
# Always use forward slashes, even on Windows.
# Don't forget to use absolute paths, not relative paths.
)


# DPN MQ SETTINGS
DPN_NODE_NAME = "aptrust" # brief name of node as configured in Federation.
DPN_BROKER_URL = 'amqp://guest:guest@localhost:5672/' #amqp://guest:guest@aptrust.duracloud.org:5672//" # amqp://guest:guest@www.mymq.org:5672//
DPN_TTL = 3600 # time in seconds for default TTL in messages
DPN_EXCHANGE = "dpn-control-exchange" # Name of DPN exchange
DPN_BROADCAST_QUEUE = "test" # Name of queue configured for DPN Federation Plugin
DPN_BROADCAST_KEY = "broadcast" # Routing key for broadcast messages
DPN_LOCAL_QUEUE = "local" # Name of local queue to bind local routing key.
DPN_LOCAL_KEY = "aptrust.dpn" # Name to use for routing direct reply messages.
DPN_XFER_OPTIONS = ['https', 'rsync'] # List of lowercase protocols available for transfer.

# DPN COMMON SETTINGS
DPN_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ" # ISO 8601 format for strftime functions.
DPN_NODE_LIST = [
    'aptrust',
    'utexas',
    'hathi',
    'sdr',
    'chron',
]
# List of allowable fixity algorithms used in DPN.
DPN_FIXITY_CHOICES = ['sha256',]

# Max Size of allowable bags
DPN_MAX_SIZE = 1099511627776 # 1 TB

# GRAPELLI SETTINGS
GRAPPELLI_ADMIN_TITLE = 'APTrust Admin'