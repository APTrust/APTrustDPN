"""
    Knowledge is knowing a tomato is a fruit; wisdom is not putting it in a
    fruit salad.

                - Miles Kington
"""

# Various utilities and helpers specific for DPN MQ
# functions.
import json
from datetime import datetime, timedelta

from dpnode.settings import DPN_DATE_FORMAT, DPN_TTL

BYTE_SYMBOLS = {
    'customary'     : ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                       'zetta', 'iotta'),
    'iec'           : ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext'       : ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                       'zebi', 'yobi'),
}

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
    
def human_to_bytes(str):
    """
    Attempts to parse a human readable byte representation based
    on default symbols and returns the corresponding bytes as an 
    integer.
    When unable to recognize the format ValueError is raised.
    """
    init = str
    num = ""
    while str and str[0:1].isdigit() or str[0:1] == '.':
        num += str[0]
        str = str[1:]
    num = float(num)
    letter = str.strip()
    for name, sset in BYTE_SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = BYTE_SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, str in enumerate(sset[1:]):
        prefix[str] = 1 << (i+1)*10
    return int(num * prefix[letter])

def json_loads(body):
    """
    Performs a json.loads operation on body, explicity converting it to string
    if it of type bytes.

    :param body: contents of a kombu.message.body
    """
    if type(body) == bytes:
        body = str(body, encoding="UTF-8")

    return json.loads(body)

def serialize_dict_date(dicc):
    """
    Serializes the date values to the DPN Date Format for a given dictionary

    :param dicc: Dictionary to be normalize
    :return: serialized date dictionary
    """

    new_dict = {}
    for key, value in dicc.items():
        if isinstance(value, datetime):
            value = dpn_strftime(value)        
        new_dict[key] = value

    return new_dict
