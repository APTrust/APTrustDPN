# Various utilities and helpers specific for DPN MQ
# functions.

from pytz import timezone

from kombu.utils import uuid

from dpnode.settings import TIME_ZONE, DPNMQ

def dpn_strftime(dt):
    """
    Returns a string formatted datetime as per the DPN message format and localized
    as per the configured value in settings.TIME_ZONE.

    :param dt: Datetime object to convert to a string.
    :return:  String of datetime with local timezone.
    """

    fmt = "%Y-%m-%d %H:%M:%S %z"
    loc_dt = timezone(TIME_ZONE).localize(dt)
    return loc_dt.strftime(fmt)

def node_uuid():
    """
    Returns a simple UUID namespaced with the node identifier as configured in settings.
    :return: String of unique ID.
    """
    return "%s-%s" % (DPNMQ["NODE"], uuid())