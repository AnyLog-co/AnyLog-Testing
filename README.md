# AnyLog Testing Suite

The following provides tests for AnyLog.

## Requirements 
* [pytest](https://pypi.org/project/pytest/)
* [pytest-xdist](https://pypi.org/project/pytest-xdist/) - to execute tests concurrently


## Test cases 
* [test_basic_query.py](tests/test_basic_query.py) - Using data of type of _ping_sensor_ execute simple aggregate functions:
  * COUNT(*)
  * COUNT(DISTINCT(value))
  * DISTINCT(value)
  * MIN(value)
  * MAX(value)
  * AVG(value)
  * SUM(value)
  * MIN(timestamp)
  * MAX(timestamp)