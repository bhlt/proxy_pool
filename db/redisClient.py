# -*- coding: utf-8 -*-
"""
-----------------------------------------------------
   File Name：     redisClient.py
   Description :   封装Redis相关操作
   Author :        JHao
   date：          2019/8/9
------------------------------------------------------
   Change Activity:
                   2019/08/09: 封装Redis相关操作
                   2020/06/23: 优化pop方法, 改用hscan命令
------------------------------------------------------
"""
__author__ = 'JHao'

from redis.connection import BlockingConnectionPool
from random import choice
from redis import Redis


class RedisClient(object):
    """
    Redis client

    Redis中代理存放的结构为hash：
    key为ip:port, value为代理属性的字典;

    """

    def __init__(self, **kwargs):
        """
        init
        :param host: host
        :param port: port
        :param password: password
        :param db: db
        :return:
        """
        self.__name = ""
        kwargs.pop("username")
        self.__conn = Redis(connection_pool=BlockingConnectionPool(
            decode_responses=True, **kwargs))

    @property
    def conn(self):
        return self.__conn

    """
    主数据库, 只有该库中的代理才会进行验证
    """
    @property
    def db_name(self):
        return self.__name

    """
    存放从代理商处下载的代理列表
    """
    @property
    def prep_db_name(self):
        return self.__name + '_prep'

    """
    存放已经从主数据库中移除的代理
    """
    @property
    def removed_db_name(self):
        return self.__name + '_removed'

    """
    从主数据库中返回一个代理
    :return:
    """
    def get(self):
        proxies = self.conn.hkeys(self.db_name)
        proxy = choice(proxies) if proxies else None
        if proxy:
            return self.conn.hget(self.using_db_name, proxy)
        else:
            return False

    """
    将代理放入prepDB的hash, 使用changeTable指定hash name
    :param proxy_obj: Proxy obj
    :return:
    """
    def put(self, proxy_obj):
        data = self.conn.hset(self.prep_db_name, proxy_obj.proxy, proxy_obj.to_json)
        return data

    """
    弹出一个代理, 先从prepDB中找，如果找到，则从prepDB中移除，同时移入到主db中
    :return: dict {proxy: value}
    """
    def pop(self):
        db = self.prep_db_name
        proxies = self.conn.hkeys(self.prep_db_name)
        if len(proxies) == 0:
            db = self.db_name
            proxies = self.conn.hkeys(self.db_name)

        for proxy in proxies:
            proxy_info = self.conn.hget(db, proxy)
            if db == self.prep_db_name:
                self.conn.hset(self.db_name, proxy, proxy_info)
                self.conn.hdel(self.prep_db_name, proxy)
            return proxy_info
        else:
            return False

    """
    从主数据库中移除指定代理, 使用changeTable指定hash name
    :param proxy_str: proxy str
    :return:
    """
    def delete(self, proxy_str):
        proxy_info = self.conn.hget(self.db_name, proxy_str)
        if proxy_info:
            self.conn.hset(self.removed_db_name, proxy_str, proxy_info)
        return self.conn.hdel(self.db_name, proxy_str)

    def exists(self, proxy_str):
        """
        判断指定代理是否存在, 使用changeTable指定hash name
        :param proxy_str: proxy str
        :return:
        """
        return self.conn.hexists(self.db_name, proxy_str)

    def update(self, proxy_obj):
        """
        更新 proxy 属性
        :param proxy_obj:
        :return:
        """
        return self.conn.hset(self.db_name, proxy_obj.proxy, proxy_obj.to_json)

    def getAll(self):
        """
        字典形式返回所有代理, 使用changeTable指定hash name
        :return:
        """
        item_dict = self.conn.hgetall(self.db_name)
        return item_dict

    def clear(self):
        """
        清空所有代理, 使用changeTable指定hash name
        :return:
        """
        return self.conn.delete(self.db_name)

    def getCount(self):
        """
        返回prepDB数据库中的代理数量
        :return:
        """
        return self.conn.hlen(self.prep_db_name)

    def changeTable(self, name):
        """
        切换操作对象
        :param name:
        :return:
        """
        self.__name = name
