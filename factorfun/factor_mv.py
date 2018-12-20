# -*- coding: utf-8 -*-
# @Time    : 2018/12/19 15:02
# @Author  : Big Huang
# @Email   : kenhuang866@qq.com
# @File    : factor_mv.py
# @Software: PyCharm
from factormanager import FactorTable
from WindPy import w
import pandas as pd
import sys

w.start()


def factor_mv(start_date, end_date):
    err_code, df = w.wsd("000005.SZ,000008.SZ,000016.SZ", "open", start_date, end_date, "", usedf=True)
    factor_name = sys._getframe().f_code.co_name
    # if err_code == -40520007:
    if err_code:
        return pd.DataFrame(columns=[FactorTable.trade_date.name, FactorTable.code.name, factor_name])
    df.index = pd.to_datetime(df.index)
    df.columns = df.columns.str[:6]
    stacked_df = df.stack(0).reset_index()
    stacked_df.columns = [FactorTable.trade_date.name, FactorTable.code.name, factor_name]
    return stacked_df


if __name__ == '__main__':
    df = factor_mv("2018-01-01", "2018-01-01")