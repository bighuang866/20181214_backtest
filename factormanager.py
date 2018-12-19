# -*- coding:utf-8 -*-

import sqlalchemy as sa
from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from sqlalchemy import create_engi                                                                                                                                                                                                                                                                                                         ne
from dbtables import ORMReprMixin
base = declarative_base()


class FactorTable(ORMReprMixin):
    code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)


def fun():
    pass


class FactorManager:
    def __init__(self, factor_engine, factor_funs):
        self.engine = factor_engine
        self.factor_funs = factor_funs
        self.factor_tables = [gen_factor_table_class(factor_fun) for factor_fun in self.factor_funs]

    def factor_names(self):
        return [factor_fun.__name__ for factor_fun in self.factor_funs]

    def qry_last_update(self, factor_table):
        sa.select([factor_table.trade_date.fun()])



def gen_factor_table_class(factor_fun: function, factor_dtype=Float):
    return type(factor_fun.__name__, (base, FactorTable,), {"__tablename__": factor_fun.__name__,
                                                            factor_fun.__name__: Column(factor_dtype)})

if __name__ == '__main__':
    FactorManager()


