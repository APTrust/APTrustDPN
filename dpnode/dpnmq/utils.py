"""
    Knowledge is knowing a tomato is a fruit; wisdom is not putting it in a
    fruit salad.

                - Miles Kington
"""

# Various utilities and helpers specific for DPN MQ
# functions.
from datetime import datetime, timedelta

from pytz import timezone

from dpnode.settings import TIME_ZONE, DPN_DATE_FORMAT, DPN_TTL

def dpn_strftime(dt):
    """
    Returns a string formatted datetime as per the DPN message format and 
    localized as per the configured value in settings.TIME_ZONE.

    :param dt: Datetime object to convert to a string.
    :return:  String of datetime with local timezone.
    """

    return dt.strftime(DPN_DATE_FORMAT)

def dpn_strptime(dt_string):
    """
    Parses a datetime object from a DPN formatted Datetime string as configured
    in localsettings.

    :param dt_string:  String in DPN datetime format to parse as a datetime object.
    :return:  Datetime object
    """
    return datetime.strptime(dt_string, DPN_DATE_FORMAT)

def is_string(obj):
    """
    Tests if an object is a string or not.
    
    :param obj:  object to be tested.
    :return:  Boolean of test result
    """
    try: 
        return isinstance(obj, str) and obj != ""
    except TypeError:
        return False

def expire_on(date, ttl=DPN_TTL):
    """
    Returns a datetime from a specific date advanced by TTL.

    :param date: datetime to calculate ttl from.
    :param ttl: time in seconds to add to the date param.
    :return: Datetime object.
    """
    return date + timedelta(0, ttl)

def str_expire_on(date, ttl=DPN_TTL):
    """
    Returns a string of the DPN format Datetime for the date provided advanded
    by the TTL

    :param date: datetime to calculate expire from.
    :param ttl:  Time in seconds to add to the date param.
    :return:  String of the formatted datetime.
    """
    return dpn_strftime(expire_on(date, ttl))