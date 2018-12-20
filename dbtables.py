# -*- coding:utf-8 -*-

# position = pd.read_excel("position.xlsx")
# position = pd.DataFrame()
# position.index.name = TRADEDATE_COL
# position.columns.name = CODE_COL
# db = cx_Oracle.connect('zszs/zszs@192.168.101.102:1521/orcl')

import sqlalchemy as sa
from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from sqlalchemy import create_engine

__all__ = ["ORMReprMixin", "sql_config_str", "BasicData", "IndexData", "Industry"]

base = declarative_base()
sql_config_str = "oracle+cx_oracle://zszs:zszs@192.168.101.102:1521/orcl"

db_date_str_format = "%Y%m%d"
class ORMReprMixin:
    def __repr__(self):
        columns = self.__class__.columns()

        elem_expr = ", ".join(["{column}={column_val}".format(column=column, column_val=self.__dict__[column].__repr__())
                               for column in columns if column in self.__dict__])

        return "{class_name}({elem_expr})".format(class_name=self.__class__.__name__,
                                                  elem_expr=elem_expr)

    @classmethod
    def columns(cls):
        return [attr_name for (attr_name, attr_value) in cls.__dict__.items()
                if isinstance(attr_value, sa.orm.attributes.InstrumentedAttribute)]

    def to_dict(self):
        return {column: self.__dict__.get(column, None) for column in self.columns()}

    def to_series(self):
        return pd.Series(self.to_dict())


    @classmethod
    def from_dict(cls, dict_obj: dict):
        return cls(**dict_obj)

    @classmethod
    def from_series(cls, ser_obj: pd.Series):
        return cls(**ser_obj.to_dict())

    @classmethod
    def from_dataframe(cls, df_obj: pd.DataFrame):
        assert set(df_obj.columns) == set(cls.columns())
        dict_list = df_obj.to_dict(orient="records")
        return [cls.from_dict(dict_obj) for dict_obj in dict_list]

    @classmethod
    def to_dataframe(cls, obj_list):
        assert all(isinstance(obj, cls) for obj in obj_list)
        return pd.DataFrame([obj.to_dict() for obj in obj_list])


class BasicData(base, ORMReprMixin):
    __tablename__ = "BASICDATA"
    # 股票代码
    code = Column("DM", String, primary_key=True)
    # 交易日期
    trade_date = Column("JYRQ", String, primary_key=True)
    # 收盘价
    close = Column("SPJ", Float)
    # 除权价
    close_xr = Column("TZHSPJ", Float)
    # 除权除息价
    close_xrxd = Column("MKPJ", Float)
    # 除权除息税后净价
    close_xrxd_net = Column("TZHCSJJ", Float)

    # 昨交易日期
    pre_trade_date = Column("ZJYRQ", String)
    # 昨收盘价
    pre_close = Column("ZSPJ", Float)
    # 昨除权价
    pre_close_xr = Column("ZTZHSPJ", Float)
    # 昨除权除息价
    pre_close_xrxd = Column("ZMKPJ", Float)
    # 昨除权除息税后净价
    pre_close_xrxd_net = Column("ZTZHCSJJ", Float)

    # 总股本
    total_shares = Column("ZGB", Integer)
    # 调整后总股本
    adjusted_total_shares = Column("TZHZGB", Integer)
    # 流通股本
    float_shares = Column("LTGB", Integer)
    # 调整后流通股本
    adjusted_float_shares = Column("TZHLTGB", Integer)
    # 分级靠档后自由流通股本
    categoty_weighted_free_float_shares = Column("JQGB", Integer)
    # 调整后分级靠档后自由流通股本
    adjusted_categoty_weighted_free_float_shares = Column("TZHJQGB", Integer)




    # 昨总股本
    pre_total_shares = Column("ZZGB", Integer)
    # 昨调整后总股本
    pre_adjusted_total_shares = Column("ZTZHZGB", Integer)
    # 昨流通股本
    pre_float_shares = Column("ZLTGB", Integer)
    # 昨调整后流通股本
    pre_adjusted_float_shares = Column("ZTZHLTGB", Integer)
    # 昨分级靠档后自由流通股本
    pre_categoty_weighted_free_float_shares = Column("ZJQGB", Integer)
    # 昨调整后分级靠档后自由流通股本
    pre_adjusted_categoty_weighted_free_float_shares = Column("ZTZHJQGB", Integer)



    # CE_总股本
    CE_total_shares = Column("CE_ZGB", Integer)
    # CE_调整后总股本
    CE_adjusted_total_shares = Column("CE_TZHZGB", Integer)
    # CE_流通股本
    CE_float_shares = Column("CE_LTGB", Integer)
    # CE_调整后流通股本
    CE_adjusted_float_shares = Column("CE_TZHLTGB", Integer)
    # CE_分级靠档后自由流通股本
    CE_categoty_weighted_free_float_shares = Column("CE_JQGB", Integer)
    # CE_调整后分级靠档后自由流通股本
    CE_adjusted_categoty_weighted_free_float_shares = Column("CE_TZHJQGB", Integer)

    # CE_昨总股本
    CE_pre_total_shares = Column("CE_ZZGB", Integer)
    # CE_昨调整后总股本
    CE_pre_adjusted_total_shares = Column("CE_ZTZHZGB", Integer)
    # CE_昨流通股本
    CE_pre_float_shares = Column("CE_ZLTGB", Integer)
    # CE_昨调整后流通股本
    CE_pre_adjusted_float_shares = Column("CE_ZTZHLTGB", Integer)
    # CE_昨分级靠档后自由流通股本
    CE_pre_categoty_weighted_free_float_shares = Column("CE_ZJQGB", Integer)
    # CE_昨调整后分级靠档后自由流通股本
    CE_pre_adjusted_categoty_weighted_free_float_shares = Column("CE_ZTZHJQGB", Integer)


    # 是否在权
    is_xr = Column("SFZQ", Integer)
    # 调整后是否在权
    is_adjusted_xr = Column("TZHSFZQ", Integer)

    # 昨是否在权
    is_pre_xr = Column("ZSFZQ", Integer)
    # 昨调整后是否在权
    is_pre_adjusted_xr = Column("ZTZHSFZQ", Integer)

    # 成交金额
    amount = Column("CJJE", Integer)
    # 成交量
    volume = Column("CJL", Integer)

    # 昨成交金额
    pre_amount = Column("ZCJJE", Integer)
    # 昨成交量
    pre_volume = Column("ZCJL", Integer)


class Industry(base, ORMReprMixin):
    __tablename__ = "SWSIND"
    code = Column("SYMBOL", String, primary_key=True)
    name = Column("NAME", String)
    sw1 = Column("IND1", String)
    sw2 = Column("IND2", String)
    sw3 = Column("IND3", String)
    csi1 = Column("IND4", String)
    csi2 = Column("IND5", String)
    csi3 = Column("IND6", String)
    csi4 = Column("IND7", String)


class IndexData(base, ORMReprMixin):
    __tablename__ = "BM"
    code = Column("SYMBOL", String, primary_key=True)
    trade_date = Column("TRADEDATE", String, primary_key=True)
    close = Column("CLOSE", Float)


if __name__ == '__main__':
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(sql_config_str, echo=True)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    # a = pd.read_sql_query(sa.select([BasicData]).where(BasicData.code=="600000").order_by(BasicData.trade_date), engine)
    # a = pd.read_sql_query(sa.select([BasicData]).
    #                       where(BasicData.code.in_(["600000", "600001"]))
    #                       .order_by(BasicData.code, BasicData.trade_date),
    #                       engine)
    #
    # b = session.query(sa.select([BasicData]).where(BasicData.code.in_(["600000", "600001"])).order_by(BasicData.code, BasicData.trade_date)).all()
    # c = session.query(BasicData).filter_by(code="600000").order_by(BasicData.code, BasicData.trade_date).all()
    # a = pd.read_sql_query("select * from BASICDATA where rownum = 1", engine)
    d = pd.read_sql_query(sa.select([BasicData.code]).where(BasicData.code=="600000"), engine)
    # d = session.query(Industry).filter_by(code="000001").order_by(Industry.code).all()
    #
    # e = pd.read_sql_query(sa.select([Industry]).where(Industry.code=="600000"), engine)
    #
    #