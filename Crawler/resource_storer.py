from bsddb3 import db, dbutils
import os

__all__ = ('Record' ,'HTMLStorer')

class Record:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        s = 'key="%s":value="%s"' % (self.key, self.value)
        return s

class ResourceStorer:
    # TODO : Sometimes Developers want to delete DB files all exactly,
    # so There need some repository to create and delete
    # also need function to delete.

    '''
    Referenced from http://egloos.zum.com/mcchae/v/10634973
    BDB Class using bsddb

    Specific Writer and Code Information.
    @package cqbsddb
    @brief wrapping class for Berkeley DB

    @version: 1.0.0
    @author: Moonchang Chae <mcchae@cqvista.com>
    '''

    def __init__(self, dbpath=None, isnew=False):
        '''
        @brief constructor
        @param dbpath [string] dbpath for berkeley DB. if None (default) then using memory repository
        '''
        self.dbtype = db.DB_BTREE  # must be set in derived class
        self.dbopenflags = db.DB_THREAD
        self.dbsetflags = 0
        self.envflags = db.DB_THREAD | db.DB_INIT_CDB | db.DB_INIT_MPOOL
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
        self.db.open(self.dbName, self.dbtype, self.dbopenflags | db.DB_CREATE)

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

    def get(self, key, isexact=False):
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

    def delete(self, key):
        '''
        @brief delete record
        @param key [string] key
        @return [bool] True on deleted otherwise False
        '''
        try:
            if self.db.exists(key):
                dbutils.DeadlockWrap(self.db.delete, key, max_retries=12)
                return True
            return False
        except:
            return False
        finally:
            pass

    def get_cursor(self):
        '''
        @brief get the db cursor
        @return [Cursor] db cursor
        '''
        return self.db.cursor()

    def close_cursor(self, cursor):
        '''
        @brief get the db cursor
        @param cursor [Cursor] db cursor
        '''
        cursor.close()

    def c_set(self, cursor, key, isexact=False):
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

    def sync(self):
        '''
        @brief sync the db
        '''
        try:
            self.db.sync()
        finally:
            pass

    def close(self):
        '''
        @brief close the db
        '''
        try:
            self.db.close()
            self.env.close()
        finally:
            pass
