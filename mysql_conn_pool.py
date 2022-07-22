# -*- coding: utf-8 -*-
#  @Author  : markadc

import pymysql
from dbutils.pooled_db import PooledDB


# 异常重试
def auto_retry(func):
    def inner(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print('Error  >>  {}  >>  {}'.format(func.__name__, e))

    return inner


class MysqlConnectionPool:
    def __init__(self, cfg, mark=False):
        self.pool = PooledDB(pymysql, **cfg)
        self.mark = mark

    def get_conn_curs(self):
        conn = self.pool.connection()
        curs = conn.cursor(pymysql.cursors.DictCursor) if self.mark else conn.cursor()
        return conn, curs

    def close_conn_curs(self, curs, conn):
        curs.close()
        conn.close()

    @auto_retry
    def exe_sql(self, sql, args=None, way=None):
        conn, curs = self.get_conn_curs()
        try:
            curs.execute(sql, args=args)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Error  >>  exe_sql  >>  {}".format(e))
            return False
        else:
            if way == 1:
                return curs.fetchone()
            elif way == 2:
                return curs.fetchall()
            else:
                return True
        finally:
            self.close_conn_curs(curs, conn)

    @auto_retry
    def exem_sql(self, sql, args: list = None):
        conn, curs = self.get_conn_curs()
        try:
            curs.executemany(sql, args=args)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Error  >>  exem_sql  >>  {}".format(e))
            return False
        else:
            return True
        finally:
            self.close_conn_curs(curs, conn)
