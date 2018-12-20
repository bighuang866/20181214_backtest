# -*- coding:utf-8 -*-

import sqlalchemy as sa
from sqlalchemy import Column, String, Integer, Float, Index
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import datetime

factor_db_date_format = "%Y%m%d"
default_start_date = datetime.datetime(2018, 1, 1)
from dbtables import ORMReprMixin
from log import logger

base = declarative_base()


class EmptyTableError(ValueError):
    pass


class FactorTable(ORMReprMixin):
    code = Column("code", String, primary_key=True)
    trade_date = Column("trade_date", String, primary_key=True)


class FactorManager:
    def __init__(self, factor_db_engine, factor_funs):
        self.factor_db_engine = factor_db_engine
        # assert isinstance(self.factor_db_engine, sa.engine.base.Engine)
        self.factor_names = [factor_fun.__name__ for factor_fun in factor_funs]
        self.factor_funs_dict = {factor_fun.__name__: factor_fun for factor_fun in factor_funs}
        self.factor_table_classes_dict = {factor_name: gen_factor_table_class(factor_name) for factor_name in
                                          self.factor_names}
        factor_names_in_db = self.factor_db_engine.table_names()
        for factor_name in factor_names_in_db:
            assert factor_name in self.factor_names
        for factor_name in self.factor_table_classes_dict:
            factor_table_class = self.factor_table_classes_dict[factor_name]
            factor_table_class.metadata.create_all(self.factor_db_engine, [factor_table_class.__table__])
        # fixme
        self.factor_index_dict = {factor_name: [] for factor_name in self.factor_names}
        logger.info("factor manager 初始化完成")
        # a = pd.read_sql_query("SELECT * FROM sqlite_master", self.factor_db_engine)

    def insert_table(self, factor_name, start_date, end_date):
        logger.info("插入因子{factor_name}数据,时间段为{st}----{et}".format(factor_name=factor_name,
                                                                  st=start_date.strftime("%Y%m%d"),
                                                                  et=end_date.strftime("%Y%m%d")))
        factor_fun = self.factor_funs_dict[factor_name]
        factor_table_class = self.factor_table_classes_dict[factor_name]
        factor_data_df = factor_fun(start_date=start_date, end_date=end_date)
        factor_data_df[factor_table_class.trade_date.name] = factor_data_df[
            factor_table_class.trade_date.name].dt.strftime(factor_db_date_format)
        factor_data_df.to_sql(factor_table_class.__tablename__,
                              self.factor_db_engine, if_exists="append", index=False)
        logger.info("插入因子{factor_name}数据完成,时间段为{st}----{et}".format(factor_name=factor_name,
                                                                    st=start_date.strftime("%Y%m%d"),
                                                                    et=end_date.strftime("%Y%m%d")))

    def qry_db_info(self):
        info_dict = {}
        for factor_name in self.factor_names:
            single_factor_info_dict = {}
            try:
                start_date = self.qry_start_date_single_table(factor_name)
                last_update_date = self.qry_last_update_single_table(factor_name)
                single_factor_info_dict["start_date"] = start_date
                single_factor_info_dict["end_date"] = last_update_date
            except EmptyTableError:
                single_factor_info_dict["start_date"] = None
                single_factor_info_dict["end_date"] = None
            info_dict[factor_name] = single_factor_info_dict
        return pd.DataFrame(info_dict).reindex(["start_date", "end_date"], axis=0).T


    def qry_start_date_single_table(self, factor_name):
        factor_table_class = self.factor_table_classes_dict[factor_name]
        qry = sa.select([sa.sql.func.min(factor_table_class.trade_date)])
        qry_result_df = pd.read_sql_query(qry, self.factor_db_engine)
        last_update_date_str = qry_result_df.squeeze()
        if last_update_date_str is None:
            raise EmptyTableError
        else:
            last_update_date = pd.Timestamp.strptime(last_update_date_str, factor_db_date_format)
            # logger.info("因子{factor_name}已更新至{last_update_date_str}".format(factor_name=factor_name,
            #                                                                      last_update_date_str=last_update_date_str))
            return last_update_date

    def qry_last_update_single_table(self, factor_name):
        factor_table_class = self.factor_table_classes_dict[factor_name]
        qry = sa.select([sa.sql.func.max(factor_table_class.trade_date)])
        qry_result_df = pd.read_sql_query(qry, self.factor_db_engine)
        last_update_date_str = qry_result_df.squeeze()
        if last_update_date_str is None:
            raise EmptyTableError
        else:
            last_update_date = pd.Timestamp.strptime(last_update_date_str, factor_db_date_format)
            # logger.info("因子{factor_name}已更新至{last_update_date_str}".format(factor_name=factor_name,
            #                                                                      last_update_date_str=last_update_date_str))
            return last_update_date

    def update_single_table(self, factor_name, end_date):
        logger.info("更新因子{factor_name}".format(factor_name=factor_name))
        last_update_date = self.qry_last_update_single_table(factor_name)
        last_update_date_plus1 = last_update_date + pd.Timedelta(days=1)
        # 只有开始时间小于结束时间
        if last_update_date_plus1 <= end_date:
            self.insert_table(factor_name, start_date=last_update_date_plus1, end_date=end_date)
            logger.info("因子{factor_name}更新完成,新插入数据时间段为{st}----{et}".format(factor_name=factor_name,
                                                                           st=last_update_date.strftime("%Y%m%d"),
                                                                           et=end_date.strftime("%Y%m%d")))
        else:
            logger.info("因子{factor_name}已更新至{last_update_date_str}, 无需继续更新".format(factor_name=factor_name,
                                                                                   last_update_date_str=last_update_date.strftime(
                                                                                       "%Y%m%d")))

    def update_all_tables(self, end_date):
        logger.info("开始更新所有因子，共有因子{factor_counts}个,更新截止日期为{end_date_str}".format(factor_counts=len(self.factor_names),
                                                                                 end_date_str=end_date.strftime("%Y%m%d")))
        for factor_name in self.factor_names:
            try:
                self.update_single_table(factor_name, end_date)
            except EmptyTableError:

                logger.info("因子{factor_name}无数据，开始插入从默认开始日期{default_start_date_str}开始的数据".format(default_start_date_str=default_start_date.strftime("%Y%m%d"),
                                                                                               factor_name=factor_name))
                self.insert_table(factor_name, start_date=default_start_date, end_date=end_date)
        logger.info("更新完成，共有因子{factor_counts}个".format(factor_counts=len(self.factor_names)))
        # todo: 加上更新的汇总统计

    def vacuum(self):
        logger.info("开始整理数据库,释放空间")
        self.factor_db_engine.execute("vacuum")
        logger.info("整理数据库完成")
    #
    # def create_index_single_table(self, factor_name):
    #     factor_index = self.gen_factor_index(factor_name)
    #     factor_index.create(bind=self.factor_db_engine)
    #     self.factor_index_dict[factor_name].append[factor_index]
    #
    # def drop_index_single_table(self, factor_name):
    #     factor_index = self.gen_factor_index(factor_name)
    #     factor_index.drop(bind=self.factor_db_engine)
    #     self.factor_index_dict[factor_name].remove[factor_index]
    #
    # def create_all_indexes(self):
    #     for factor_name in self.factor_names:
    #         factor_index_list = self.factor_index_dict[factor_name]
    #         if not factor_index_list:
    #             self.create_index_single_table(factor_name)
    #
    # def drop_all_indexes(self):
    #     for factor_name in self.factor_names:
    #         factor_index_list = self.factor_index_dict[factor_name]
    #         if factor_index_list:
    #             self.drop_index_single_table(factor_name)
    #
    # def gen_factor_index(self, factor_name):
    #     factor_table_class = self.factor_table_classes_dict[factor_name]
    #     return Index("%s_index" % factor_name, factor_table_class.code, factor_table_class.trade_date)


def gen_factor_table_class(factor_name, factor_dtype=Float):
    return type(factor_name, (base, FactorTable,), {"__tablename__": factor_name,
                                                    factor_name: Column(factor_dtype)})


if __name__ == '__main__':
    # import sqlite3
    # sqlite3.connect("factor_data.db")
    from sqlalchemy import create_engine
    import datetime
    from factorfun.factor_mv import factor_mv

    factor_engine = create_engine(r"sqlite:///E:\CSI\20181214_backtest\factor_data.db", echo=False)
    factor_manager = FactorManager(factor_engine, [factor_mv])
    # factor_manager.insert_table("factor_mv", datetime.datetime(2018, 1, 1), datetime.datetime(2018, 1, 30))
    # factor_manager.qry_last_update_single_table("factor_mv")
    # factor_manager.qry_last_update_single_table("factor_mv")
    # factor_manager.update_all_tables(datetime.datetime(2018, 2, 15))
    # factor_manager.create_all_indexes()
    factor_manager.qry_db_info()