# -*- coding: utf-8 -*-

# All function related to database manipulation
# from __future__ import absolute_import

import MySQLdb
import MySQLdb.cursors

import logging
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from .config import DB_SERVER, DB_USER, DB_PASS, DB_NAME

# Basic database functions
class base_db(object):

	def __init__(self):
		self.conn = MySQLdb.connect(DB_SERVER, DB_USER, DB_PASS, DB_NAME, 
	                	charset="utf8", use_unicode=True, cursorclass=MySQLdb.cursors.DictCursor)
		self.cursor = self.conn.cursor()


	class Decorators(object):
		@classmethod
		def retry_db_errors(self, function):
			def db_func_wrapper(base_db, sql_string, sql_vars=(), debug_sql=0):
				try: return function(base_db, sql_string, sql_vars, debug_sql)
				except (AttributeError, MySQLdb.OperationalError) as e: 
					logging.log(logging.WARNING, "Retrying... Found an error in a mysql execution %d: %s" % (e.args[0], e.args[1]))
					base_db.__init__()
					raise e

			return db_func_wrapper

	# Simplification to execute an SQL string of getting a data from the database
	@retry(retry=retry_if_exception_type((AttributeError, MySQLdb.OperationalError)), 
		stop=stop_after_attempt(7), wait=wait_fixed(60))
	@Decorators.retry_db_errors
	def get(self, sql_string, sql_vars, debug_sql):
		self.cursor.execute(sql_string, sql_vars)
		if debug_sql: print (self.cursor._last_executed)
		return self.cursor.fetchall()

	# Simplification to execute an SQL string of inserting or updating a data to the database
	@retry(retry=retry_if_exception_type((AttributeError, MySQLdb.OperationalError)), 
		stop=stop_after_attempt(7), wait=wait_fixed(2))
	@Decorators.retry_db_errors
	def set(self, sql_string, sql_vars, debug_sql):
		self.cursor.execute(sql_string, sql_vars)
		if debug_sql: print (self.cursor._last_executed)
		self.conn.commit()
		if "UPDATE" in sql_string: return self.cursor.rowcount
		else: return self.cursor.lastrowid

	def disconnect(self):
		'''Disconnect from the MySQL server'''
		if self.conn is not None:
			logging.log(logging.INFO, "Closing MySQL connection")
			self.conn.close()
			self.conn = None
			self.cursor = None