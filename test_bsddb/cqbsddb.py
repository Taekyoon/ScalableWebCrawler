#!/usr/bin/env python

'''
@package cqbsddb
@brief wrapping class for Berkeley DB

@version: 1.0.0
@author: Moonchang Chae <mcchae@cqvista.com>

license: CQVista's NDA

@note this package bsddb3 for thread-safe
	$ sudo apt-get install libdb-dev
	$ wget http://pypi.python.org/packages/source/b/bsddb3/bsddb3-5.1.1.tar.gz#md5=c13e47c18077f96381d4fb6ca18f4181
	$ python setup.py build && sudo python setup.py install 
'''

__author__ = "MoonChang Chae <mcchae@cqvista.com>"
__date__ = "2010/12/24"
__version__ = "1.0.0"
__version_info__ = (1, 0, 0)
__license__ = "CQVista's NDA"

import os
import random
import datetime
import threading
import unittest
from bsddb3 import db, dbutils

###################################################################################################
class Record:
	#==============================================================================================
	def __init__(self, key, value):
		self.key = key
		self.value = value
	#==============================================================================================
	def __repr__(self):
		s = 'key="%s":value="%s"' % (self.key, self.value)
		return s

###################################################################################################
class BDB:
	#==============================================================================================
	def __init__(self, dbpath=None, isnew = False):
		'''
		@brief constructor
		@param dbpath [string] dbpath for berkeley DB. if None (default) then using memory repository
		'''
		self.dbtype	   = db.DB_BTREE  # must be set in derived class
		self.dbopenflags  = db.DB_THREAD
		self.dbsetflags   = 0
		self.envflags	 = db.DB_THREAD | db.DB_INIT_CDB | db.DB_INIT_MPOOL
		if dbpath:
			self.homeDir = os.path.dirname(dbpath)
			if not os.path.exists(self.homeDir):
				os.makedirs(self.homeDir)
			self.dbName = os.path.basename(dbpath)
		else:
			self.homeDir = None
			self.dbName = None
		self.env = db.DBEnv()
		self.env.open(self.homeDir, self.envflags | db.DB_CREATE)
		self.db = db.DB(self.env)
		if self.dbsetflags: self.db.set_flags(self.dbsetflags)
		self.db.open(self.dbName, self.dbtype, self.dbopenflags|db.DB_CREATE)
		
	#==============================================================================================
	def put(self, key, value, isoverwrite=True):
		'''
		@brief put key, value pair
		@param key [string] key
		@param value [string] value
		@return [bool] True or False (cannot overwrite)
		'''
		try:
			if not isoverwrite and self.db.exists(key):
				return False
			dbutils.DeadlockWrap(self.db.put, key, value, max_retries=12)
			return True
		except:
			return False
		finally:
			pass
	#==============================================================================================
	def exists(self, key):
		'''
		@brief db has the or not
		@param key [string] key
		@return [bool] True if has key otherwise False
		'''
		try:
			return self.db.exists(key)
		finally:
			pass
	#==============================================================================================
	def get(self, key, isexact = False):
		'''
		@brief get the value which have the key
		@param key [string] key
		@param isexact [bool] if True then return only (key, value) otherwise next (key, value)
		@return [Record] Record(key, value) or None
		'''
		try:
			if isexact and not self.db.exists(key): return None
			return Record(key, self.db.get(key))
		finally:
			pass
	#==============================================================================================
	def delete(self, key):
		'''
		@brief delete record
		@param key [string] key
		@return [bool] True on deleted otherwise False
		'''
		try:
			if self.db.exists(key):
				#self.db.delete(key)
				dbutils.DeadlockWrap(self.db.delete, key, max_retries=12)
				return True
			return False
		except:
			return False
		finally:
			pass
	#==============================================================================================
	def get_cursor(self):
		'''
		@brief get the db cursor
		@return [Cursor] db cursor
		'''
		return self.db.cursor()
	#==============================================================================================
	def close_cursor(self, cursor):
		'''
		@brief get the db cursor
		@param cursor [Cursor] db cursor
		'''
		cursor.close()
	
	#==============================================================================================
	def c_set(self, cursor, key, isexact = False):
		'''
		@brief set the (key, value) tuple at the cursor
		@param cursor [Cursor] db cursor
		@param key [string] key to search
		@param isexact [bool] if True the use cursor.set else use cursor.set_range
		@return [Record] Record(key, value) or None
		'''
		try:
			if isexact:
				keyval = cursor.set(key)
			else:
				keyval = cursor.set_range(key)
			return Record(keyval[0], keyval[1])
		except:
			return None
		finally:
			pass
	#==============================================================================================
	def c_next(self, cursor, prefix=None):
		'''
		@brief get the next (key, value) tuple at the cursor
		@param cursor [Cursor] db cursor
		@param prefix [string] if key is not starts with prefix return (None, None)
		@return [Record] Record(key, value) or None
		'''
		try:
			keyval = cursor.next()
			if prefix and not keyval[0].startswith(prefix):
				return None
			return Record(keyval[0], keyval[1])
		except:
			return None
		finally:
			pass
	#==============================================================================================
	def c_first(self, cursor):
		'''
		@brief get the first (key, value) tuple at the cursor
		@param cursor [Cursor] db cursor
		@return [Record] Record(key, value) or None
		'''
		try:
			keyval = cursor.first()
			return Record(keyval[0], keyval[1])
		except:
			return None
		finally:
			pass
	#==============================================================================================
	def c_last(self, cursor):
		'''
		@brief get the last (key, value) tuple at the cursor
		@param cursor [Cursor] db cursor
		@return [Record] Record(key, value) or None
		'''
		try:
			keyval = cursor.last()
			return Record(keyval[0], keyval[1])
		except:
			return None
		finally:
			pass
	#==============================================================================================
	def c_previous(self, cursor):
		'''
		@brief get the previous (key, value) tuple at the cursor
		@param cursor [Cursor] db cursor
		@return [Record] Record(key, value) or None
		'''
		try:
			keyval = cursor.previous()
			return Record(keyval[0], keyval[1])
		except:
			return None
		finally:
			pass
	#==============================================================================================
	def stat_nkeys(self):
		'''
		@brief return the number of total keys (valid only if BTREE and RECNO)
		@return [int]
		'''
		try:
			return self.db.stat()["nkeys"]
		except:
			return None
		finally:
			pass
	#==============================================================================================
	def sync(self):
		'''
		@brief sync the db
		'''
		try:
			self.db.sync()
		finally:
			pass
	#==============================================================================================
	def close(self):
		'''
		@brief close the db
		'''
		try:
			#self.db.close()
			self.db.close()
			self.env.close()
		finally:
			pass

###################################################################################################
def putTest(bdb, startcnt, endcnt):
	sts = datetime.datetime.now()
	rvallist = range(startcnt, endcnt+1); random.shuffle(rvallist)
	for rval in rvallist:
		key = '%09d' % rval
		value = '%09d_%09d' % (rval, rval)
		bdb.put(key, value)
	ets = datetime.datetime.now()
	print "[%s] BDB %d put takes %s" % (threading.current_thread().getName(), endcnt-startcnt+1, ets-sts)
	print '[%s] Current number of Rec = %d' % (threading.current_thread().getName(), bdb.stat_nkeys())

###################################################################################################
def getTest(bdb, startcnt, endcnt, totalcnt = 0):
	fcnt = 0
	sts = datetime.datetime.now()
	
	if totalcnt <= 0:
		totalcnt = endcnt - startcnt + 1
		rvallist = range(startcnt, endcnt+1); random.shuffle(rvallist)
		for rval in rvallist:
			key = '%09d' % rval
			rec = bdb.get(key)
			if rec: fcnt += 1
	else:
		for _ in range(totalcnt):
			rval = random.randrange(startcnt, endcnt)
			key = '%09d' % rval
			rec = bdb.get(key)
			if rec: fcnt += 1
	
	ets = datetime.datetime.now()
	print "[%s] BDB %d found from %d get takes %s" % (threading.current_thread().getName(), fcnt, totalcnt, ets-sts)
	print '[%s] Current number of Rec = %d' % (threading.current_thread().getName(), bdb.stat_nkeys())
	return fcnt

###################################################################################################
def existsTest(bdb, startcnt, endcnt):
	fcnt = 0
	sts = datetime.datetime.now()
	rvallist = range(startcnt, endcnt+1); random.shuffle(rvallist)
	for rval in rvallist:
		key = '%09d' % rval
		if bdb.exists(key):
			fcnt += 1
	ets = datetime.datetime.now()
	print "[%s] BDB %d found from %d exists takes %s" % (threading.current_thread().getName(), fcnt, endcnt-startcnt+1, ets-sts)
	print '[%s] Current number of Rec = %d' % (threading.current_thread().getName(), bdb.stat_nkeys())
	return fcnt

###################################################################################################
def cursorTest(bdb, startcnt, endcnt):
	sts = datetime.datetime.now()
	cr = bdb.get_cursor()
	key = '%09d' % (startcnt + (endcnt - startcnt)/2)
	crcnt = 0
	rec = bdb.c_set(cr, key)
	while rec:
		crcnt += 1
		if crcnt <= 3:
			print '\t%s' % rec
		rec = bdb.c_next(cr)
	bdb.close_cursor(cr)
	ets = datetime.datetime.now()
	print "[%s] BDB %d cursor <%d>traverse takes %s" % (threading.current_thread().getName(), endcnt-startcnt+1, crcnt, ets-sts)
	print '[%s] Current number of Rec = %d' % (threading.current_thread().getName(), bdb.stat_nkeys())
	return crcnt

###################################################################################################
def deleteTest(bdb, startcnt, endcnt):
	sts = datetime.datetime.now()
	rvallist = range(startcnt, endcnt+1); random.shuffle(rvallist)
	for rval in rvallist:
		key = '%09d' % rval
		bdb.delete(key)
	ets = datetime.datetime.now()
	print "[%s] BDB %d delete takes %s" % (threading.current_thread().getName(), endcnt-startcnt+1, ets-sts)
	print '[%s] Current number of Rec = %d' % (threading.current_thread().getName(), bdb.stat_nkeys())

###################################################################################################
def threadTest(bdb):
	import cqthread

	class putThread (cqthread.Thread):
		def __init__(self, bdb, startcnt, endcnt):
			cqthread.Thread.__init__(self)
			self.bdb = bdb
			self.startcnt = startcnt
			self.endcnt = endcnt
		def run(self):
			putTest(self.bdb, self.startcnt, self.endcnt)
	class getThread (cqthread.Thread):
		def __init__(self, bdb, startcnt, endcnt, totalcnt):
			cqthread.Thread.__init__(self)
			self.bdb = bdb
			self.startcnt = startcnt
			self.endcnt = endcnt
			self.totalcnt = totalcnt
		def run(self):
			getTest(self.bdb, self.startcnt, self.endcnt, self.totalcnt)
	class existsThread (cqthread.Thread):
		def __init__(self, bdb, startcnt, endcnt):
			cqthread.Thread.__init__(self)
			self.bdb = bdb
			self.startcnt = startcnt
			self.endcnt = endcnt
		def run(self):
			existsTest(self.bdb, self.startcnt, self.endcnt)
	class deleteThread (cqthread.Thread):
		def __init__(self, bdb, startcnt, endcnt):
			cqthread.Thread.__init__(self)
			self.bdb = bdb
			self.startcnt = startcnt
			self.endcnt = endcnt
		def run(self):
			deleteTest(self.bdb, self.startcnt, self.endcnt)
			
	tlist = []	
	t = putThread(bdb, 100001, 200000); tlist.append(t)
	t = putThread(bdb, 200001, 300000); tlist.append(t)
	t = putThread(bdb, 300001, 400000); tlist.append(t)
	t = putThread(bdb, 400001, 500000); tlist.append(t)
	t = putThread(bdb, 500001, 600000); tlist.append(t)
	t = putThread(bdb, 600001, 700000); tlist.append(t)
	t = putThread(bdb, 700001, 800000); tlist.append(t)
	t = putThread(bdb, 800001, 900000); tlist.append(t)
	t = getThread(bdb, 100001, 300000, 100000); tlist.append(t)
	t = getThread(bdb, 300001, 500000, 100000); tlist.append(t)
	t = getThread(bdb, 500001, 700000, 100000); tlist.append(t)
	
	for t in tlist:
		t.start()

	for t in tlist:
		t.join()
	
	print 'Total number of Rec = %d' % bdb.stat_nkeys()
	
##################################################################################################
g_bdb = None
class testUnit (unittest.TestCase):
	'''
	@brief test case using uniitest.TestCase
	'''
	def setUp(self):
		'''
		@brief this method is called before every test methods
		'''
		self.dbpath = '/tmp/bdb/dbtest.bdb'
		global g_bdb
		if g_bdb == None:
			if os.path.exists(self.dbpath): 
				os.system('rm -rf "%s"' % os.path.dirname(self.dbpath))
			g_bdb = BDB(self.dbpath)
		self.bdb = g_bdb
		self.startcnt = 1000001
		self.endcnt = 2000000
		self.totalcnt = self.endcnt - self.startcnt + 1
		pass
	def tearDown(self):
		'''
		@brief this method is called after every test methods
		'''
		pass
	def test010_putTest(self):
		print '%s putTest' % ('=' * 60)
		putTest(self.bdb, self.startcnt, self.endcnt)
		self.assertTrue(self.bdb.stat_nkeys() == self.totalcnt)
	def test020_getTest(self):
		print '%s getTest' % ('=' * 60)
		rc = getTest(self.bdb, self.startcnt, self.endcnt)
		self.assertTrue(self.bdb.stat_nkeys() == rc)
	def test030_existsTest(self):
		print '%s existsTest' % ('=' * 60)
		rc = existsTest(self.bdb, self.startcnt, self.endcnt)
		self.assertTrue(self.bdb.stat_nkeys() == rc)
	def test040_cursorTest(self):
		print '%s cursorTest' % ('=' * 60)
		rc = cursorTest(self.bdb, self.startcnt, self.endcnt)
		self.assertTrue(rc == self.totalcnt/2 + 1)
	def test050_deleteTest(self):
		print '%s deleteTest' % ('=' * 60)
		deleteTest(self.bdb, self.startcnt, self.endcnt)
		self.assertTrue(self.bdb.stat_nkeys() == 0)
	def test060_threadTest(self):
		print '%s threadTest' % ('=' * 60)
		threadTest(self.bdb)
		self.assertTrue(self.bdb.stat_nkeys() == 800000)

###################################################################################################
if __name__ == '__main__':
	suite = unittest.TestLoader().loadTestsFromTestCase(testUnit)
	unittest.TextTestRunner(verbosity=2).run(suite)	
	
