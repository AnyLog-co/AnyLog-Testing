# AnyLog Testing Suite

## Test Cases
* `pytest_base_querise` - Based a on a data set of 25,862 rows on a single table, validate SQL funcitons and timezone
  1. Data is inserted using rest PUT, as such the node should already have an empty anylog database
  2. Test basic aggregate functions:
      * `COUNT` - against columns: _*_, _timestamp_ and _value_ 
      * `MIN` - _timestamp_ and _value_
      * `MAX` - _timestamp_ and _value_
      * `AVG` - _value_
      * `SUM` - _value_
  3. `ORDER BY` conditions: 
      * raw 
      * `ASC`
      * `DESC`
  4. `WHERE` conditions: 
     * less than
     * greater than
     * range of timestamps between a single day 
     * range of timeestamps betweeen 2 days
     * variable
  5. `GROUP BY` 
  6. `increments` conditions: 
    * minute - 1, 10, 30, 60
    * hour - 1, 6, 12, 24
    * day - 1, 3, 5, 7 
    * with `GROUP BY` 
    * with `WHERE` condition - both mid-day and in-between days
  7. `period` conditions:
    * minute - 1, 10, 30, 60
    * hour - 1, 6, 12, 24
    * day - 1, 3, 5, 7 
    * historic dates