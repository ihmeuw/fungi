""" Parsing and writing dates and times. """

import datetime
import math

__author__ = "Vince Reuter"
__modified__ = "2016-11-15"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.dates_times"


class DT(object):
    """ Handle datetime parsing and writing. """

    # TODO: provide method that absorbs dev plugin as alternative.
    # TODO: the builtin ES datetime format that it looks like we want to use is "date_time" (also, "strict_date_time")
    # TODO: we could also perhaps truncate the millis and use "date_time_no_millis" or "strict_date_time_no_millis"
    # TODO: use the ES guide on date ranges: https://www.elastic.co/guide/en/elasticsearch/guide/current/_ranges.html#_ranges_on_dates


    _REQUIRED_MEMBER_NAMES = ("parse", "write")
    _DATE_TIME_DELIMITER = "T"

    _ZONE_SUFFIX = "Z"

    _PARTIAL_SECONDS_DELIMITER = "."
    _DATE_DELIMITER = "-"
    _TIME_DELIMITER = ":"


    @classmethod
    def parse(cls, dt_text):
        """
        Parse the given datetime-encoding text to produce datetime instance.

        >>> str(parse("2016-11-06T11:04:20.897Z"))
        '2016-11-06 11:04:20.897000'

        :param str dt_text: datetime-encoding text
        :return datetime.datetime: datetime instance encoded by given text
        """

        d, t = dt_text.strip().split(cls._DATE_TIME_DELIMITER)

        year, month, day = map(int, d.split(cls._DATE_DELIMITER))

        hour, minute, seconds = t.split(cls._TIME_DELIMITER)
        hour, minute = int(hour), int(minute)

        seconds = seconds[-len(cls._ZONE_SUFFIX)]
        seconds, msecs = seconds.split(cls._PARTIAL_SECONDS_DELIMITER)
        seconds = int(seconds)
        microseconds = 1000 * msecs

        dt = datetime.datetime(year, month, day, hour,
                               minute, seconds, microseconds)
        return dt


    @classmethod
    def write(cls, dt):
        """
        Represent given datetime instance as text.

        :param datetime.datetime dt: datetime instance to write as text
        :return str: text representation of given datetime instance
        """
        pass


    @classmethod
    def is_valid(cls, alternative):
        """
        Determine if given class or instance suffices as alternative to this.

        :param object alternative: class or instance proposed as datetime
            handler to function as alternative to this one
        :return bool: flag indicating whether given object
            suffices as alternative to this
        """
        for required_member_name in cls._REQUIRED_MEMBER_NAMES:
            if not hasattr(alternative, required_member_name):
                return False
        return True
