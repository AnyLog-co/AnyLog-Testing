# AnyLog Testing Suite

The following provides tests for AnyLog.

```bash
python3 -m pytest tests/${TEST_NAME} -vn #
```

## Requirements 
* [pytest](https://pypi.org/project/pytest/)
* [pytest-xdist](https://pypi.org/project/pytest-xdist/) - to execute tests concurrently


## Test cases 
* [test_litsanleandro_single_table_queries.py.py](tests/test_litsanleandro_single_table_queries.py) - Validate behavior of SQL via AnyLog against a single table (data set: _ping_sensor_)
* [test_litsanleandro_multi_table_queries.py](tests/test_litsanleandro_multi_table_queries.py) - Validate behavior of SQL via AnyLog against using `include` function without `extend` (data set: _ping_sensor_, _percentagecpu_sensor_)
* [test_litsanleandro_multi_table_extend_queries.py](tests/test_litsanleandro_multi_table_extend_queries.py) - Validate behavior of SQL via AnyLog against using `include` and `extend` functions (data set: _ping_sensor_, _percentagecpu_sensor_)
* [test_blockchain.py](tests/test_blockchain.py) - Testing blockchain functions, including adding data / validating data, different cases with `blockchain get` and removing polciies   
