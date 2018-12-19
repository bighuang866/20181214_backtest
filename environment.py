# -*- coding: utf-8 -*-
# @Time    : 2018/11/30 14:20
# @Author  : Big Huang
# @Email   : kenhuang866@qq.com
# @File    : environment.py
# @Software: PyCharm Community Edition
from sqlalchemy import create_engine
from dbtables import base, sql_config_str
from sqlalchemy.orm import sessionmaker
import os
import sqlite3
__all__ = ["Environment"]


class Environment:
    _env = None

    def __init__(self):
        if Environment._env is None:
            Environment._env = self
        else:
            raise RuntimeError("Environment不可初始化多次")
        print("环境初始化，数据库链接")
        self.engine = create_engine(sql_config_str, echo=True)
        # base.metadata.create_all(self.engine)
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()

    @classmethod
    def get_instance(cls):
        if cls._env is None:
            raise RuntimeError("Environment未初始化")
        else:
            assert isinstance(cls._env, Environment)
            return cls._env


env = Environment()
