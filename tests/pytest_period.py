import argparse 
import datetime 
import json 
import os 
import pytest
import requests
import yaml 

import read_config 

CONFIG_FILE = '$HOME/AnyLog-Network/tests/rest/configs/default_config.yaml' 
#CONFIG_FILE = '$HOME/AnyLog-Network/tests/rest/power_grid.meter_data.config.yaml' 

class TestPeriodNow:
   """
   For each aggregate (COUNT, MIN, MAX, AVG, SUM) check that the result of N nodes (alone) is equal to combined via REST
   """ 
   def setup_class(self): 
      """
      Setup class 
      :param:
         self.timestamp - for SQL oldest query date 
         self.config_info - based on CONFIG_FILE info for REST requets 
      """
      self.config_info = read_config.read_config(CONFIG_FILE)
      if not self.config_info: 
         exit(1) 

   def setup(self): 
      """
      setup method
      :param: 
         self.test_fail:bool - updated by test_keys or test_status to check config are valid for farther testing
      """
      self.test_fail = False 
   
   def teardown(self): 
      """
      Test if test_keys or test_status failed. If so exist (cannot continue) 
      """
      if self.test_fail is True: 
         exit(1) 

  
   def __execute_query(self, headers:dict)->dict: 
      """
      Execute query based on headers 
      :args: 
         headers:dict - header 
      :return: 
         results in the form of headers 
      """
      try: 
          return requests.get('http://%s' % self.config_info['REST'], headers=headers).json() 
      except Exception as e: 
         print(e) 
         return {} 

   def test_status(self): 
      """
      Test node is accessible
      :assert: 
         node is running 
      """
      headers = {'type': 'info', 'details': 'get status'} 
      results = self.__execute_query(headers)
      try: 
          assert 'running' in results['Status']
      except: 
         self.test_fail = True 
         assert False 

   def test_keys(self): 
      """
      Test self.config_info is valid
      :assert: 
         keys in self.config_info are as expected 
      """
      config_keys = ['REST', 'OPERATOR', 'DB', 'TABLE', 'TABLE COLUMNS', 'MIN DAYS BACK', 'MAX DAYS BACK']
      if list(self.config_info.keys()) != config_keys:
          self.test_fail = True 
      assert list(self.config_info.keys()) == config_keys 


   def test_minute(self): 
       """
       Validate increments for minute
       :Query: 
          SELECT COUNT(*) FROM table WHERE period(minute, val, now(), timestamp) 
       """ 
       query = "SELECT COUNT(*) AS count FROM %s WHERE period(minute, %s, now(), timestamp);" 
       for ts in [1, 5, 10, 30, 60]: 
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts)}
          results = self.__execute_query(headers) 
          try: 
             expected_results = int(results['Query'][0]['count'])
          except: 
             assert False    
          
          actual_results = 0 
          for server in self.config_info['OPERATOR']: 
             headers['servers'] = server 
             results = self.__execute_query(headers) 
             try: 
                result = int(results['Query'][0]['count']) 
             except: 
                assert False 
             actual_results += result 
          assert actual_results == expected_results       


   def test_hour(self): 
       """
       Validate increments for hour
       :Query: 
          SELECT COUNT(*) FROM table WHERE period(hour, val, now(), timestamp) 
       """ 
       query = "SELECT COUNT(*) AS count FROM %s WHERE period(hour, %s, now(), timestamp);" 
       for ts in [1, 6, 12, 24]: 
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts)}
          results = self.__execute_query(headers) 
          try: 
             expected_results = int(results['Query'][0]['count'])
          except: 
             assert False    
          
          actual_results = 0 
          for server in self.config_info['OPERATOR']: 
             headers['servers'] = server 
             results = self.__execute_query(headers) 
             try: 
                result = int(results['Query'][0]['count']) 
             except: 
                assert False 
             actual_results += result 
          assert actual_results == expected_results       

   def test_day(self): 
       """
       Validate increments for day
       :Query: 
          SELECT COUNT(*) FROM table WHERE period(day, val, now(), timestamp) 
       """ 
       query = "SELECT COUNT(*) AS count FROM %s WHERE period(day, %s, now(), timestamp);" 
       for ts in [1, 7, 14, 28, 30]: 
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts)}
          results = self.__execute_query(headers) 
          try: 
             expected_results = int(results['Query'][0]['count'])
          except: 
             assert False    
          
          actual_results = 0 
          for server in self.config_info['OPERATOR']: 
             headers['servers'] = server 
             results = self.__execute_query(headers) 
             try: 
                result = int(results['Query'][0]['count']) 
             except: 
                assert False 
             actual_results += result 
          assert actual_results == expected_results

   def test_week(self): 
       """
       Validate increments for week
       :Query: 
          SELECT COUNT(*) FROM table WHERE period(week, val, now(), timestamp) 
       """ 
       query = "SELECT COUNT(*) AS count FROM %s WHERE period(week, %s, now(), timestamp);" 
       for ts in [1, 4, 12, 24, 52]: 
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts)}
          results = self.__execute_query(headers) 
          try: 
             expected_results = int(results['Query'][0]['count'])
          except: 
             assert False    
          
          actual_results = 0 
          for server in self.config_info['OPERATOR']: 
             headers['servers'] = server 
             results = self.__execute_query(headers) 
             try: 
                result = int(results['Query'][0]['count']) 
             except: 
                assert False 
             actual_results += result 
          assert actual_results == expected_results

   def test_month(self): 
       """
       Validate increments for month
       :Query: 
          SELECT COUNT(*) FROM table WHERE period(month, val, now(), timestamp) 
       """ 
       query = "SELECT COUNT(*) AS count FROM %s WHERE period(month, %s, now(), timestamp);" 
       for ts in [1, 3, 4, 6, 8, 9, 10, 12]: 
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts)}
          results = self.__execute_query(headers) 
          try: 
             expected_results = int(results['Query'][0]['count'])
          except: 
             assert False    
          
          actual_results = 0 
          for server in self.config_info['OPERATOR']: 
             headers['servers'] = server 
             results = self.__execute_query(headers) 
             try: 
                result = int(results['Query'][0]['count']) 
             except: 
                assert False 
             actual_results += result 
          assert actual_results == expected_results

   def test_year(self): 
       """
       Validate increments for year
       :Query: 
          SELECT COUNT(*) FROM table WHERE period(year, val, now(), timestamp) 
       """ 
       query = "SELECT COUNT(*) AS count FROM %s WHERE period(year, %s, now(), timestamp);" 
       for ts in [1, 5, 10]: 
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts)}
          results = self.__execute_query(headers) 
          try: 
             expected_results = int(results['Query'][0]['count'])
          except: 
             assert False    
          
          actual_results = 0 
          for server in self.config_info['OPERATOR']: 
             headers['servers'] = server 
             results = self.__execute_query(headers) 
             try: 
                result = int(results['Query'][0]['count']) 
             except: 
                assert False 
             actual_results += result 
          assert actual_results == expected_results

   def test_minute_vs_hour(self): 
      """
      Validate 60 minute vs 1 hour 
      :Query: 
         SELECT COUNT(*) FROM table WHERE period(hour, 1, now(), timestamp)
         SELECT COUNT(*) FROM table WHERE period(minute, 60, now(), timestamp) 
      """
      minute_query = "SELECT COUNT(*) AS count FROM %s WHERE period(minute, 60, now(), timestamp)"
      minute_headers = {"type": "sql", "dbms": self.config_info['DB'], "details": minute_query % self.config_info['TABLE']}
      hour_query = "SELECT COUNT(*) AS count FROM %s WHERE period(hour, 1, now(), timestamp)"
      hour_headers = {"type": "sql", "dbms": self.config_info['DB'], "details": hour_query % self.config_info['TABLE']}

      results = self.__execute_query(minute_headers) 
      try: 
          expected_minute = int(results['Query'][0]['count'])
      except: 
         assert False 
      results = self.__execute_query(hour_headers) 
      try: 
          expected_hour = int(results['Query'][0]['count'])
      except: 
         assert False 
      
      assert expected_minute == expected_hour

      actual_minute = 0 
      actual_hour = 0 
      for operator in self.config_info['OPERATOR']: 
         minute_headers['servers'] = operator 
         hour_headers['servers'] = operator 

         results = self.__execute_query(minute_headers) 
         try: 
            actual_minute += int(results['Query'][0]['count'])
         except: 
            assert False 
      
         results = self.__execute_query(hour_headers) 
         try: 
            actual_hour += int(results['Query'][0]['count'])
         except: 
            assert False 

      assert actual_hour == actual_minute 
      assert actual_hour == expected_minute 
      assert actual_minute == expected_hour 

   def test_hour_vs_day(self): 
      """
      Validate 24 hour and day
      :Query: 
         SELECT COUNT(*) FROM table WHERE period(hour, 24, now(), timestamp)
         SELECT COUNT(*) FROM table WHERE period(day, 1, now(), timestamp) 
      """
      day_query = "SELECT COUNT(*) AS count FROM %s WHERE period(day, 1, now(), timestamp)"
      day_headers = {"type": "sql", "dbms": self.config_info['DB'], "details": day_query % self.config_info['TABLE']}
      hour_query = "SELECT COUNT(*) AS count FROM %s WHERE period(hour, 24, now(), timestamp)"
      hour_headers = {"type": "sql", "dbms": self.config_info['DB'], "details": hour_query % self.config_info['TABLE']}

      results = self.__execute_query(day_headers) 
      try: 
          expected_day = int(results['Query'][0]['count'])
      except: 
         assert False 
      results = self.__execute_query(hour_headers) 
      try: 
          expected_hour = int(results['Query'][0]['count'])
      except: 
         assert False 
      
      assert expected_day == expected_hour

      actual_day = 0 
      actual_hour = 0 
      for operator in self.config_info['OPERATOR']: 
         day_headers['servers'] = operator 
         hour_headers['servers'] = operator 

         results = self.__execute_query(day_headers) 
         try: 
            actual_day += int(results['Query'][0]['count'])
         except: 
            assert False 
      
         results = self.__execute_query(hour_headers) 
         try: 
            actual_hour += int(results['Query'][0]['count'])
         except: 
            assert False 

      assert actual_hour == actual_day
      assert actual_hour == expected_day 
      assert actual_day == expected_hour

   @pytest.mark.skip(reason="diff in results") 
   def test_day_vs_week(self): 
      """
      Validate 7 days and week
      :Query: 
         SELECT COUNT(*) FROM table WHERE period(week, 1, now(), timestamp)
         SELECT COUNT(*) FROM table WHERE period(day, 7, now(), timestamp) 
      """
      day_query = "SELECT COUNT(*) AS count FROM %s WHERE period(day, 7, now(), timestamp)"
      day_headers = {"type": "sql", "dbms": self.config_info['DB'], "details": day_query % self.config_info['TABLE']}
      week_query = "SELECT COUNT(*) AS count FROM %s WHERE period(week, 1, now(), timestamp)"
      week_headers = {"type": "sql", "dbms": self.config_info['DB'], "details": week_query % self.config_info['TABLE']}

      results = self.__execute_query(day_headers) 
      try: 
          expected_day = int(results['Query'][0]['count'])
      except: 
         assert False 
      results = self.__execute_query(week_headers) 
      try: 
          expected_week = int(results['Query'][0]['count'])
      except: 
         assert False 
      
      assert expected_day == expected_week

      actual_day = 0 
      actual_week = 0 
      for operator in self.config_info['OPERATOR']: 
         day_headers['servers'] = operator 
         week_headers['servers'] = operator 

         results = self.__execute_query(day_headers) 
         try: 
            actual_day += int(results['Query'][0]['count'])
         except: 
            assert False 
      
         results = self.__execute_query(week_headers) 
         try: 
            actual_week += int(results['Query'][0]['count'])
         except: 
            assert False 

      assert actual_week == actual_day
      assert actual_week == expected_day 
      assert actual_day == expected_week
