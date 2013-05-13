# Various utilities and helpers specific for DPN MQ
# functions.
from datetime import datetime

from pytz import timezone

from dpnode.settings import TIME_ZONE, DPNMQ

def dpn_strftime(dt):
    """
    Returns a string formatted datetime as per the DPN message format and 
    localized as per the configured value in settings.TIME_ZONE.

    :param dt: Datetime object to convert to a string.
    :return:  String of datetime with local timezone.
    """

    loc_dt = timezone(TIME_ZONE).localize(dt)
    return loc_dt.strftime(DPNMQ['DT_FMT'])

def dpn_strptime(dt_string):
    """
    Parses a datetime object from a DPN formatted Datetime string as configured
    in localsettings.

    :param dt_string:  String in DPN datetime format to parse as a datetime object.
    :return:  Datetime object
    """
    return datetime.strptime(dt_string, DPNMQ['DT_FMT'] )

def is_string(obj):
    """
    Tests if an object is a string or not.
    
    :param obj:  object to be tested.
    :return:  Boolean of test result
    """
    try: 
        return isinstance(obj, basestring) and obj != ""
    except TypeError:
        return False