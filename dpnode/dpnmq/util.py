# Various utilities and helpers specific for DPN MQ
# functions.

from pytz import timezone

from dpnode.settings import TIME_ZONE

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