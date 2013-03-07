"""
Overview
========

DPNMQ is a django app to handle AMQP messaging within the Digital Preservation Network.

"""

# Set some version info for the package.
# Format is major, minor, sub release versions and the final is for dev or not.
# example ... (0, 1, 0, "dev") will produce a __version__ value of "0.1.0-dev"
__version_info__ = (0, 1, 0, "dev")

# Dot-connect all but the last. Last is dash-connected if not None.
__version__ = '.'.join([ str(i) for i in __version_info__[:-1] ])
if __version_info__[-1] is not None:
    __version__ += ('-%s' % (__version_info__[-1],))