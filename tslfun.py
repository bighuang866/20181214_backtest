# -*- coding: utf-8 -*-
# @Time    : 2018/12/20 16:07
# @Author  : Big Huang
# @Email   : kenhuang866@qq.com
# @File    : tslfun.py
# @Software: PyCharm
import pandas as pd
import sys
import datetime
sys.path.append(r"E:\software\Analyse.NET")
import TSLPy3 as ts
ts.ConnectServer("tsl.tinysoft.com.cn", 443)
dl = ts.LoginServer("indexsh","888888") #Tuple(ErrNo,ErrMsg) 登陆用户
assert dl[0] == 0
print("登陆成功")
print("服务器设置:", ts.GetService())
ts.SetComputeBitsOption(64)  # 设置计算单位
print("计算位数设置:", ts.GetComputeBitsOption())

def tsl_fun(start_date: datetime.datetime, end_date:datetime.datetime):
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    # data = ts.RemoteExecute("return 'return a string';",{}) #执行一条语句
    data = ts.RemoteCallFunc("big", [start_date_str, end_date_str, "SH000050"], {})
    df = pd.DataFrame(data[1])
    df.columns = df.columns.map(lambda x: x.decode("gbk"))
    for col, dtype in df.dtypes.iteritems():
        if dtype.name == "object":
            df[col] = df[col].str.decode("gbk")



if __name__ == '__main__':
    start_date = datetime.datetime(2018, 12, 18)
    end_date = datetime.datetime(2018, 12, 19)

