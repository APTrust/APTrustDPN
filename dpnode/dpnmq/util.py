# Various utilities and helpers specific for DPN MQ
# functions.


def dpn_strftime(dt):
    """
    Returns a string formatted datetime as per the DPN message format.

    :param dt: Datetime object to convert to a string.
    """

    # "2013-01-18 09:49:28 -0800"
    fmt = "%Y-%m-%d %H:%M:%S %z"
    return dt.strftime(fmt)