#!/usr/bin/env python3
# coding:utf-8
"""
__title__ = ''
__author__ = 'David Ao'
__mtime__ = '2018/6/12'
# 
"""
import logging
import redis
import sys
import platform

import pymysql as moduledb


class RedisManage:
    """
    redis
    """

    def __init__(self):
        host = '192.168.199.29'
        port = '6379'
        self.r = redis.StrictRedis(host, port)

    def get_task(self, key):
        """
        
        :type key:
        :return: 
        """
        try:
            if platform.system() == 'Windows':
                with open('config', 'r') as f:
                    flag = f.readline()
                    if flag != 'run':
                        return None
            url = self.r.rpop(key)
        except Exception as e:
            return None

        if url is None:
            return None
        url = url.decode().strip()
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://' + url

        return url

    def get_task_iter(self, key):
        """

        :type key:
        :return: 
        """
        while 1:
            try:
                url = self.r.rpop(key)
            except Exception as e:
                break

            if url is None:
                break
            url = url.decode()
            if not url.startswith('http://') and not url.startswith('https://'):
                url = 'http://' + url
            yield url


class MySqlManage:
    def __init__(self, logger=None, long_conn=False):
        self.host = '192.168.199.29'
        self.port = 3306
        self.database = 'ai'
        self.user = 'root'
        self.password = 'ZMa.123456'
        self.charset = 'utf8'
        self.logger = logger
        self.long_conn = long_conn
        self.conn = None
        if long_conn:
            self.conn = self.connect()
        if logger:
            logger.info('created a mysql connection')

    def connect(self):
        """
        
        :return: 
        """
        conn = moduledb.connect(host=self.host,
                                port=self.port,
                                database=self.database,
                                user=self.user,
                                password=self.password,
                                charset=self.charset)
        conn.autocommit(0)

        return conn

    def execute(self, *args, **kwargs):
        """
        长连接的情况下不关闭conn
        :param args: 
        :param kwargs: 
        :return: 
        """
        conn = self.conn if self.conn else self.connect()
        try:
            cursor = conn.cursor()
            try:
                sql = args[0].strip()
                if not (sql.startswith("SELECT") or sql.startswith("select")):
                    cursor.execute("START TRANSACTION")
                    ret = cursor.execute(*args, **kwargs)
                    cursor.execute("COMMIT")
                else:
                    ret = cursor.execute(*args, **kwargs)
            except Exception as e:
                cursor.execute("ROLLBACK")
                if self.logger:
                    self.logger.exception(e)
                    self.logger.info(args)
                raise e
            finally:
                cursor.close()
        finally:
            if not self.long_conn:
                conn.close()

        return ret


def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler = logging.FileHandler('scrapy_log.log', encoding="UTF-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # stream_handler = logging.StreamHandler(sys.stderr)
    # logger.addHandler(stream_handler)

    return logger
