# Set a reasonable Project Path setting so I dont' have to use hard coded paths.
import os
PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))

DEBUG = False # Always make False by default.
TEMPLATE_DEBUG = DEBUG
TRAVIS_ENV = False

# Travis-ci settings
if 'TRAVIS' in os.environ:
    DEBUG = True
    TEMPLATE_DEBUG = True
    TRAVIS_ENV = True

# CELERY WORKER CONFIG
BROKER_URL = 'amqp://guest:guest@localhost:5672/'
# CELERYD_CONCURRENCY = 8 # set # of max concurrent workers or defaults to cpus

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

# Use for travis-ci tests only
if TRAVIS_ENV:
    DATABASES = {
        'default': {
            'ENGINE':   'django.db.backends.postgresql_psycopg2',
            'NAME':     'travisdb',  # Must match travis.yml setting
            'USER':     'postgres',
            'PASSWORD': '',
            'HOST':     'localhost',
            'PORT':     '',
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
TIME_ZONE = 'UTC'

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
DPN_EXCHANGE = "dpn-control-exchange" # Name of DPN exchange
DPN_BROADCAST_QUEUE = "test" # Name of queue configured for DPN Federation Plugin
DPN_BROADCAST_KEY = "broadcast" # Routing key for broadcast messages
DPN_LOCAL_QUEUE = "local" # Name of local queue to bind local routing key.
DPN_LOCAL_KEY = "aptrust.dpn" # Name to use for routing direct reply messages.
DPN_XFER_OPTIONS = ['https', 'rsync'] # List of lowercase protocols available for transfer.
DPN_NUM_XFERS = 1 # Number of nodes to choose for transfers.

DPN_TTL = 3600 # default time in seconds for default TTL in messages
DPN_MSG_TTL = {
    'replication-init-query': 10,
    'replication-available-reply': 10,
    'replication-location-reply': 10,
    'replication-location-cancel': 3600,
    'replication-transfer-reply': 3600,
    'replication-verify-reply': 3600,
    'registry-item-create': 10,
}

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

# Directory to be monitored for new added bags
DPN_INGEST_DIR_OUT = os.path.join(PROJECT_PATH, '../dpn_bags_dir')

# Directory to store files that are going to be recovered by other node.
DPN_RECOVERY_DIR_OUT = os.path.join(PROJECT_PATH, '../dpn_recovery_dir_out')

DPN_BAGS_FILE_EXT = 'tar' # Default DPN bag file extension

# Absolute filesystem path to the directory where bags will be replicated.
# Example: "/home/media/dpn.aptrust/bags.root/"
DPN_REPLICATION_ROOT = ''

DPN_BASE_LOCATION = {
    'https': 'https://dpn.aptrust.org/outbound/',
    'rsync': 'dpn@dpn.aptrust.org:/outbound/',
}

DPN_RECOVER_LOCATION = {
    'https': 'https://dpn.aptrust.org/recovery/',
    'rsync': 'dpn@dpn.aptrust.org:/recovery/'
}

DPN_DEFAULT_XFER_PROTOCOL = DPN_XFER_OPTIONS[0] # default HTTPS

PROTOCOL_LIST = list(DPN_BASE_LOCATION.keys())

# GRAPELLI SETTINGS
GRAPPELLI_ADMIN_TITLE = 'APTrust Admin'


# Setup local logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'filters': {
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
            },
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        }
    },
    'handlers': {
        'null': {
            'level': 'DEBUG',
            'class': 'django.utils.log.NullHandler',
            },
        'console':{
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'console_info':{
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'mail_admins': {
            'level': 'ERROR',
            'class': 'django.utils.log.AdminEmailHandler',
            'filters': ['require_debug_false'],
        }
    },
    'loggers': {
        'django': {
            'handlers': ['null'],
            'propagate': True,
            'level': 'INFO',
            },
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': False,
            },
        'dpnmq.consumer': {
            'handlers': ['console_info'],
            'level': 'INFO',
            'filters': ['require_debug_true'],
        },
        'dpnmq.producer': {
            'handlers': ['console_info'],
            'level': 'INFO',
            'filters': ['require_debug_true'],
        },
        'dpnmq.console': {
            'handlers': ['console_info'],
            'level': 'INFO',
            'filters': ['require_debug_true'],
            },
    },
}
