import argparse 
import datetime 
import json 
import os 
import pytest
import requests
import yaml 
import read_config 

CONFIG_FILE = '$HOME/AnyLog-Network/tests/rest/configs/default_config.yaml'

class TestValidateData: 
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

   def __order_columns(self)->dict: 
       data = {
          'timestamp': [], 
          'string': [], 
          'numeric': [] 
       }
       for col in self.config_info['TABLE COLUMNS']:
          data[self.config_info['TABLE COLUMNS'][col]].append(col) 
       return data 
  
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
         assert False  

   def __get_count(self): 
      """
      Get raw row cunt
      :query:  
         SELECT COUNT(*) FROM %s;
      """
      headers = {"type": "sql", "dbms": self.config_info['DB'], "details": "SELECT COUNT(*) AS count FROM %s" % self.config_info['TABLE']} 
      results = self.__execute_query(headers) 
      return int(results['Query'][0]['count'])

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
      :assert: 
         validate all relevent keys exit
      """
      config_keys = ['REST', 'OPERATOR', 'DB', 'TABLE', 'TABLE COLUMNS', 'MIN DAYS BACK', 'MAX DAYS BACK']
      if list(self.config_info.keys()) != config_keys:
          self.test_fail = True 
      assert list(self.config_info.keys()) == config_keys 
      
   def test_combined_aggreggate(self): 
      """
      Assert multiple aggregates in a single query 
      :query: 
         SELECT MIN(num), MAX(num), AVG(num), SUM(num) FROM table_name; 
       :assert: 
         min
         max 
         avg = sum/count
         sum = sum(sum) 
         count = sum(count)    
      """
      query = "SELECT MIN(%s) AS min_%s, MAX(%s) AS max_%s, AVG(%s) AS avg_%s, SUM(%s) AS sum_%s, COUNT(%s) AS count_%s FROM %s;" 
      headers = {"type": "sql", "dbms": self.config_info['DB'], 'details': query} 
      
      ordered_columns = self.__order_columns()

      for col in ordered_columns['numeric']: 
         header = headers 
         header['details'] = header['details'] % (col, col, col, col, col, col, col, col, col, col, self.config_info['TABLE'])

         results = self.__execute_query(header) 
         expected_results = results['Query'][0]
         
         for result in expected_results: 
             if '.' in expected_results[result]: 
                expected_results[result] = float(expected_results[result])
             else: 
                expected_results[result] = int(expected_results[result])

         actual_results = {} 
         for server in self.config_info['OPERATOR']:
            header['servers'] = server 
            results = self.__execute_query(header) 
            for result in results['Query'][0]: 
                if result not in actual_results: 
                    actual_results[result] = [] 
                if '.' in results['Query'][0][result]:
                   actual_results[result].append(float(results['Query'][0][result]))
                else: 
                   actual_results[result].append(int(results['Query'][0][result]))



         assert min(actual_results['min_%s' % col]) == expected_results['min_%s' % col]
         assert max(actual_results['max_%s' % col]) == expected_results['max_%s' % col]
         assert sum(actual_results['sum_%s' % col]) / sum(actual_results['count_%s' % col]) == expected_results['avg_%s' % col]
         assert sum(actual_results['sum_%s' % col]) == expected_results['sum_%s' % col] 
         assert sum(actual_results['count_%s' % col]) == expected_results['count_%s' % col]


   def test_combined_aggreggate_col(self): 
      """
      Assert multiple aggregates + col query in a single query
      :query: 
         SELECT col, MIN(num), MAX(num), AVG(num), SUM(num) FROM table_name GROUP BY col ORDER BY col
       :assert: 
         min
         max 
         avg = sum/count
         sum = sum(sum) 
         count = sum(count)    
      """
      query = "SELECT %s, MIN(%s) AS min_%s, MAX(%s) AS max_%s, AVG(%s) AS avg_%s, SUM(%s) AS sum_%s, COUNT(%s) AS count_%s FROM %s GROUP BY %s;" 
      headers = {"type": "sql", "dbms": self.config_info['DB'], 'details': ''} 
      expected_results = {} 
      actual_results = {} 

      ordered_columns = self.__order_columns()
      group_by_list = [] 
      for col_type in ordered_columns: 
          if col_type != 'numeric': 
              for col in ordered_columns[col_type]: 
                  group_by_list.append(col) 
                  
      for col in ordered_columns['numeric']:
         for gcol in group_by_list:
            header = headers 
            header['details'] = query % (gcol, col, col, col, col, col, col, col, col, col, col, self.config_info['TABLE'], gcol)

            results = self.__execute_query(header)

            for result in results['Query']:
               expected_results[result[gcol]] = {'count_%s' % col: int(result['count_%s' % col])}
               if '.' in  result['min_%s' % col]:
                  expected_results[result[gcol]]['min_%s' % col] = float(result['min_%s' % col])
               else: 
                  expected_results[result[gcol]]['min_%s' % col] = int(result['min_%s' % col])
               if '.' in  result['max_%s' % col]:
                  expected_results[result[gcol]]['max_%s' % col] = float(result['max_%s' % col])
               else: 
                  expected_results[result[gcol]]['max_%s' % col] = int(result['max_%s' % col])
 
               if '.' in  result['avg_%s' % col]:
                  expected_results[result[gcol]]['avg_%s' % col] = float(result['min_%s' % col])
               else: 
                  expected_results[result[gcol]]['avg_%s' % col] = int(result['min_%s' % col])
               if '.' in  result['sum_%s' % col]:
                  expected_results[result[gcol]]['sum_%s' % col] = float(result['max_%s' % col])
               else: 
                  expected_results[result[gcol]]['sum_%s' % col] = int(result['sum_%s' % col])

         for server in self.config_info['OPERATOR']:
            header['servers'] = server 
            results = self.__execute_query(header)
            for result in results['Query']: 
               if result[gcol] not in actual_results: 
                  actual_results[result[gcol]] = {'min_%s' % col: [], 'max_%s' % col: [], 'avg_%s' % col: [], 'sum_%s' % col: [], 'count_%s' % col: []} 

               actual_results[result[gcol]]['count_%s' % col].append(int(result['count_%s' % col]))
               if '.' in  result['min_%s' % col]:
                  actual_results[result[gcol]]['min_%s' % col].append(float(result['min_%s' % col]))
               else:
                  actual_results[result[gcol]]['min_%s' % col].append(int(result['min_%s' % col]))
               if '.' in  result['max_%s' % col]:
                  actual_results[result[gcol]]['max_%s' % col].append(float(result['max_%s' % col]))
               else:
                  actual_results[result[gcol]]['max_%s' % col].append(int(result['max_%s' % col]))
               if '.' in  result['avg_%s' % col]:
                  actual_results[result[gcol]]['avg_%s' % col].append(float(result['min_%s' % col]))
               else:
                  actual_results[result[gcol]]['avg_%s' % col].append(int(result['min_%s' % col]))
               if '.' in  result['sum_%s' % col]:
                  actual_results[result[gcol]]['sum_%s' % col].append(float(result['max_%s' % col]))
               else:
                  actual_results[result[gcol]]['sum_%s' % col].append(int(result['sum_%s' % col]))

         for result in actual_results: 
             assert  result in expected_results
             assert min(actual_results[result]['min_%s' % col]) == expected_results[result]['min_%s' % col] 
             assert max(actual_results[result]['max_%s' % col]) == expected_results[result]['max_%s' % col] 
             assert sum(actual_results[result]['sum_%s' % col]) == expected_results[result]['sum_%s' % col]
             assert sum(actual_results[result]['count_%s' % col]) == expected_results[result]['count_%s' % col]

   def test_count_less_than(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp < value
      :query:
         SELECT COUNT(*) FROM table where ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp < '%s';" 
      timestamps  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]
      for timestamp in timestamps:
         headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], timestamp)}
         results = self.__execute_query(headers) 
         try: 
            expected_results = int(results['Query'][0]['count'])
         except: 
            expected_results = results['Query.output.all']

         actual_results = []
         for server in self.config_info['OPERATOR']: 
            headers['servers'] = server 
            results = self.__execute_query(headers)
            try: 
               actual_results.append(int(results['Query'][0]['count']))
            except: 
               actual_results.append(results['Query.output.all'])

         if all(isinstance(result, int) for result in actual_results): 
            assert sum(actual_results) == expected_results 
         elif all(isinstance(result, str) for result in actual_results): 
            assert all(result == expected_results for result in actual_results)
         else:
            actual_result = 0 
            for result in actual_results: 
               if isinstance(result, int): 
                  actual_result += result 
            assert actual_result == expected_results   

   def test_count_less_than_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp <= value
      :query:
         SELECT COUNT(*) FROM table where ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp <= '%s';" 
      timestamps  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]
      for timestamp in timestamps:
         headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], timestamp)}
         results = self.__execute_query(headers) 
         try: 
            expected_results = int(results['Query'][0]['count'])
         except: 
            expected_results = results['Query.output.all']

         actual_results = []
         for server in self.config_info['OPERATOR']: 
            headers['servers'] = server 
            results = self.__execute_query(headers)
            try: 
               actual_results.append(int(results['Query'][0]['count']))
            except: 
               actual_results.append(results['Query.output.all'])

         if all(isinstance(result, int) for result in actual_results): 
            assert sum(actual_results) == expected_results 
         elif all(isinstance(result, str) for result in actual_results): 
            assert all(result == expected_results for result in actual_results)
         else:
            actual_result = 0 
            for result in actual_results: 
               if isinstance(result, int): 
                  actual_result += result 
            assert actual_result == expected_results

   def test_count_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp = value
      :query:
         SELECT COUNT(*) FROM table where ts = value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp = '%s';" 
      timestamps  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]
      for timestamp in timestamps:
         headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], timestamp)}
         results = self.__execute_query(headers) 
         try: 
            expected_results = int(results['Query'][0]['count'])
         except: 
            expected_results = results['Query.output.all']

         actual_results = []
         for server in self.config_info['OPERATOR']: 
            headers['servers'] = server 
            results = self.__execute_query(headers)
            try: 
               actual_results.append(int(results['Query'][0]['count']))
            except: 
               actual_results.append(results['Query.output.all'])

         if all(isinstance(result, int) for result in actual_results): 
            assert sum(actual_results) == expected_results 
         elif all(isinstance(result, str) for result in actual_results): 
            assert all(result == expected_results for result in actual_results)
         else:
            actual_result = 0 
            for result in actual_results: 
               if isinstance(result, int): 
                  actual_result += result 
            assert actual_result == expected_results

   def test_count_greater_than_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value
      :query:
         SELECT COUNT(*) FROM table where ts >= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s';" 
      timestamps  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]
      for timestamp in timestamps:
         headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], timestamp)}
         results = self.__execute_query(headers) 
         try: 
            expected_results = int(results['Query'][0]['count'])
         except: 
            expected_results = results['Query.output.all']

         actual_results = []
         for server in self.config_info['OPERATOR']: 
            headers['servers'] = server 
            results = self.__execute_query(headers)
            try: 
               actual_results.append(int(results['Query'][0]['count']))
            except: 
               actual_results.append(results['Query.output.all'])

         if all(isinstance(result, int) for result in actual_results): 
            assert sum(actual_results) == expected_results 
         elif all(isinstance(result, str) for result in actual_results): 
            assert all(result == expected_results for result in actual_results)
         else:
            actual_result = 0 
            for result in actual_results: 
               if isinstance(result, int): 
                  actual_result += result 
            assert actual_result == expected_results

   def test_count_greater_than(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value
      :query:
         SELECT COUNT(*) FROM table where ts > value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s';" 
      timestamps  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]
      for timestamp in timestamps:
         headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], timestamp)}
         results = self.__execute_query(headers) 
         try: 
            expected_results = int(results['Query'][0]['count'])
         except: 
            expected_results = results['Query.output.all']

         actual_results = []
         for server in self.config_info['OPERATOR']: 
            headers['servers'] = server 
            results = self.__execute_query(headers)
            try: 
               actual_results.append(int(results['Query'][0]['count']))
            except: 
               actual_results.append(results['Query.output.all'])

         if all(isinstance(result, int) for result in actual_results): 
            assert sum(actual_results) == expected_results 
         elif all(isinstance(result, str) for result in actual_results): 
            assert all(result == expected_results for result in actual_results)
         else:
            actual_result = 0 
            for result in actual_results: 
               if isinstance(result, int): 
                  actual_result += result 
            assert actual_result == expected_results

   def test_count_greater_than_and_less_than(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value AND timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts > value AND ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' AND timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_and_less_than_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value AND timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts > value AND ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' AND timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_or_less_than(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value OR timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts > value OR ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' OR timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_or_less_than_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value OR timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts > value OR ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' AND timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_and_less_than(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value AND timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value AND ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' AND timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_and_less_than_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value AND timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value AND ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' AND timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_or_less_than(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value OR timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value OR ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' OR timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_or_less_than_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value OR timestamp < value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value OR ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' AND timestamp < '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_and_less_than_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value AND timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value AND ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' AND timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_and_less_than_equals_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value AND timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value AND ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' AND timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_or_less_than_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value OR timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value OR ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' OR timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_equals_or_less_than_equals_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp >= value OR timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts >= value OR ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp >= '%s' AND timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_and_less_than_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value AND timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts > value AND ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' AND timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_and_less_than_equals_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value AND timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts > value AND ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' AND timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_or_less_than_equals(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value OR timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts > value OR ts < value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' OR timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps1: 
         for ts2 in timestamps2:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_greater_than_or_less_than_equals_inverse(self): 
      """
      GET COUNT(*) WHERE insert_tiimestamp > value OR timestamp <= value 
      :query:
         SELECT COUNT(*) FROM table where ts > value OR ts <= value
      """
      query = "SELECT COUNT(*) AS count FROM %s WHERE timestamp > '%s' AND timestamp <= '%s';" 
      timestamps1  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MIN DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MIN DAYS BACK']),
      ]
      timestamps2  = [
              datetime.datetime.today() - datetime.timedelta(hours=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(days=self.config_info['MAX DAYS BACK']),
              datetime.datetime.today() - datetime.timedelta(weeks=self.config_info['MAX DAYS BACK'])
      ]

      for ts1 in timestamps2: 
         for ts2 in timestamps1:
            headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], ts1, ts2)}
            results = self.__execute_query(headers)
            try: 
               expected_results = int(results['Query'][0]['count'])
            except: 
               expected_results = results['Query.output.all'] 

            actual_results = []
            for server in self.config_info['OPERATOR']: 
               headers['servers'] = server 
               results = self.__execute_query(headers)
               try: 
                  actual_results.append(int(results['Query'][0]['count']))
               except: 
                  actual_results.append(results['Query.output.all'])

            if all(isinstance(result, int) for result in actual_results): 
               assert sum(actual_results) == expected_results 
            elif all(isinstance(result, str) for result in actual_results): 
               assert all(result == expected_results for result in actual_results)
            else:
               actual_result = 0 
               for result in actual_results: 
                  if isinstance(result, int): 
                     actual_result += result 
               assert actual_result == expected_results

   def test_count_col_equals(self): 
       """
       Validate equals returns correct results
       :query: 
           SELECT  COUNT(*) FROM table WHERE col = val
       """
       sub_query = "SELECT DISTINCT(%s) AS %s FROM %s LIMIT 10;" 
       query = "SELECT COUNT(*) AS count FROM %s WHERE %s = '%s';" 
       for column in self.config_info['TABLE COLUMNS']:
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": sub_query % (column, column, self.config_info['TABLE'])}
          results = self.__execute_query(headers) 
          for value in results['Query']: 
             key = list(value.keys())[0] 
             val = value[key]
             headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], key, val)}
             results = self.__execute_query(headers)
             expected_results = int(results['Query'][0]['count']) 

             actual_results = []
             for server in self.config_info['OPERATOR']:
                headers['servers'] = server
                results = self.__execute_query(headers)
                try:
                   actual_results.append(int(results['Query'][0]['count']))
                except:
                   actual_results.append(results['Query.output.all'])

             if all(isinstance(result, int) for result in actual_results):
                assert sum(actual_results) == expected_results
             elif all(isinstance(result, str) for result in actual_results):
                assert all(result == expected_results for result in actual_results)
             else:
                actual_result = 0
                for result in actual_results:
                   if isinstance(result, int):
                      actual_result += result
                assert actual_result == expected_results
                assert actual < self.__get_count() 

   @pytest.mark.skip(reason='not supported') 
   def test_count_col_not_equals(self): 
       """
       Validate not equals returns correct results 
       :query: 
          SELECT COUNT(*) FROM table WHERE col != val 
       """
       sub_query = "SELECT DISTINCT(%s) AS %s FROM %s LIMIT 10;" 
       query = "SELECT COUNT(*) AS count FROM %s WHERE %s <> '%s';" 
       for column in self.config_info['TABLE COLUMNS']:
          headers = {"type": "sql", "dbms": self.config_info['DB'], "details": sub_query % (column, column, self.config_info['TABLE'])}
          results = self.__execute_query(headers) 
          for value in results['Query']: 
             key = list(value.keys())[0] 
             val = value[key]
             headers = {"type": "sql", "dbms": self.config_info['DB'], "details": query % (self.config_info['TABLE'], key, val)}
             results = self.__execute_query(headers)
             print(results) 
             expected_results = int(results['Query'][0]['count']) 

             actual_results = []
             for server in self.config_info['OPERATOR']:
                headers['servers'] = server
                results = self.__execute_query(headers)
                try:
                   actual_results.append(int(results['Query'][0]['count']))
                except:
                   actual_results.append(results['Query.output.all'])

             if all(isinstance(result, int) for result in actual_results):
                assert sum(actual_results) == expected_results
             elif all(isinstance(result, str) for result in actual_results):
                assert all(result == expected_results for result in actual_results)
             else:
                actual_result = 0
                for result in actual_results:
                   if isinstance(result, int):
                      actual_result += result
                assert actual_result == expected_results
                assert actual < self.__get_count() 
                assert expected_results < self.__get_count()
