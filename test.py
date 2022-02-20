import datetime
from dateutil import parser
import time

_datetime = "2018-03-30T12:34:56.789Z"
datetime_obj = parser.parse(_datetime)
print(datetime_obj.timestamp())
print(type(datetime_obj))
