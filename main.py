# -*- coding:utf-8 -*-

from dbtables import BasicData, db_date_str_format
from sqlalchemy import create_engine
from enum import Enum
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from cached_property import cached_property
from pandas.tseries.offsets import CustomBusinessDay, CustomBusinessMonthBegin, CustomBusinessMonthEnd
import sqlalchemy as sa
import datetime
import pandas as pd
from environment import Environment
from settings import IGNORE_AHEAD_DELIST, BASE_POINT



class FormDataCol(Enum):
    index_code = "指数代码"
    start_date = "开始日期"
    end_date = "结束日期"
    stock_code = "证券代码"
    weight_factor = "权重因子"


class InstrumentInfoCol(Enum):
    code = "code"
    list_date = "list_date"
    delist_date = "delist_date"


class TradeDatesMixin:
    # def check_date_range(self, start_para, end_para):
    #     def decorator(func):
    #         @functools.wraps(func)
    #         def wrapper(*args, **kwargs):
    #             if not args:
    #                 st = kwargs[start_para]
    #                 et = kwargs[end_para]
    #             elif len(args) == 1 and kwargs:
    #                 st = args[0]
    #                 et = kwargs[end_para]
    #             elif len(args) >= 2:
    #                 st = args[0]
    #                 et = args[1]
    #             if not (self.is_in_trade_calendar_range(st) and self.is_in_trade_calendar_range(et)):
    #                 raise ValueError("输入开始日期与结束日期非法")
    #             return func(*args, **kwargs)
    #
    #         return wrapper
    #
    #     return decorator

    @cached_property
    def trade_calendar(self):
        """
        交易日历
        :return:
        """
        raise NotImplementedError

    @property
    def natural_calendar_start_date(self):
        """
        自然日历开始时间
        :return:
        """
        return NotImplementedError

    @property
    def natural_calendar_end_date(self):
        """
        自然日历结束时间
        :return:
        """
        return NotImplementedError

    @property
    def trade_calendar_start_date(self):
        """
        交易日历开始时间
        :return:
        """
        return self.trade_calendar[0]

    @property
    def trade_calendar_end_date(self):
        """
        交易日历结束时间
        :return:
        """
        return self.trade_calendar[-1]

    @cached_property
    def natural_calendar(self):
        """
        自然日历
        :return:
        """
        return pd.date_range(self.natural_calendar_start_date, self.natural_calendar_end_date, freq="D")

    @cached_property
    def holiday_calendar(self):
        """
        节假日日历
        :return:
        """
        return self.natural_calendar.difference(self.trade_calendar)

    @cached_property
    def trade_date_offset(self):
        """
        交易日offset
        :return:
        """
        return CustomBusinessDay(holidays=self.holiday_calendar.tolist())

    @cached_property
    def trade_month_begin_offset(self):
        """
        每月第一个交易日offset
        :return:
        """
        return CustomBusinessMonthBegin(holidays=self.holiday_calendar.tolist())

    @cached_property
    def trade_month_end_offset(self):
        """
        每月最后一个交易日offset
        :return:
        """
        return CustomBusinessMonthEnd(holidays=self.holiday_calendar.tolist())

    def is_in_trade_calendar_range(self, date: datetime.datetime):
        """
        判断是否为交易日
        :param date:
        :return:
        """
        return self.trade_calendar_start_date <= date <= self.trade_calendar_end_date

    # def trade_date_shift(self, date, bias):
    #     """
    #
    #     :param date:
    #     :param bias:
    #     :return:
    #     """
    #     result_date = date + self.trade_date_offset*bias
    #     if self.is_in_trade_calendar_range(result_date):
    #         return result_date
    #     else:
    #         raise Exception("结果日期超出日期范围")

    # # @check_date_range("start", "end")
    # def ndate_range(self, *args, **kwargs):
    #     return pd.date_range(*args, **kwargs, freq="D")
    #
    # # @check_date_range("start", "end")
    # def tdate_range(self, *args, **kwargs):
    #     return pd.date_range(*args, **kwargs, freq=self.trade_date_offset)
    #
    # # @check_date_range("start", "end")
    # def tmon_start_range(self, *args, **kwargs):
    #     return pd.date_range(*args, **kwargs, freq=self.trade_month_begin_offset)
    #
    # # @check_date_range("start", "end")
    # def tmon_end_range(self, *args, **kwargs):
    #     return pd.date_range(*args, **kwargs, freq=self.trade_month_end_offset)


class InstrumentMixin:
    @cached_property
    def instrument_info_df(self):
        """
        :return:
        """
        raise NotImplementedError

    def get_listing_instrument(self, date):
        is_list = self.instrument_info_df[InstrumentInfoCol.list_date.value] <= date
        not_delist = self.instrument_info_df[InstrumentInfoCol.delist_date.value] >= date
        return self.instrument_info_df.index[is_list & not_delist].tolist()


class BackTestEngine(InstrumentMixin, TradeDatesMixin):
    env = Environment.get_instance()

    def __init__(self, form_data: pd.DataFrame):
        # 将所有成分股的开始日期，如果其不是交易日，将其转换为之后的最近的第一个交易日
        form_data[FormDataCol.start_date.value] = form_data[FormDataCol.start_date.value].map(
            self.trade_date_offset.rollforward)
        # 将所有成分股的结束时间，如果其不是交易日，转换为之前的最近的第一个交易日
        form_data[FormDataCol.start_date.value] = form_data[FormDataCol.start_date.value].map(
            self.trade_date_offset.rollback)

        start_date_illegals = []
        end_date_illegals = []
        # 检查有没有上市和退市
        # fixme: 效率可以再提升一些
        for ind, _, start_date, end_date, stock_code, _ in form_data.itertuples():
            info_ser = self.instrument_info_df.loc[stock_code, :]
            list_date = info_ser[InstrumentInfoCol.list_date.value]
            delist_date = info_ser[InstrumentInfoCol.delist_date.value]
            # 为何开始时间后不带=，结束时间后带=
            # list_date和delist_date是该合约在数据库里有数据的里的第一天和最后一天
            # 而开始日期时需要使用前一天的数据，因此开始日期需要大于而不是大于等于list_date
            if start_date <= list_date:
                start_date_illegals.append([stock_code, start_date, list_date])
            if end_date > delist_date:
                end_date_illegals.append([stock_code, end_date, delist_date])
                form_data.loc[ind, FormDataCol.end_date.value] = delist_date

        if start_date_illegals:
            raise ValueError
        if end_date_illegals and not IGNORE_AHEAD_DELIST:
            raise ValueError

        index_start_date = form_data[FormDataCol.start_date.value].min()
        index_end_date = form_data[FormDataCol.end_date.value].max()
        all_codes = form_data[FormDataCol.stock_code.value].unique().tolist()
        index_start_date_dbformat = index_start_date.strftime(db_date_str_format)
        index_end_date_dbformat = index_end_date.strftime(db_date_str_format)
        # fixme: oracle最多支持长度1000的list

        pre_close_col = BasicData.pre_close_xr

        data_qry = sa.select([BasicData.code, BasicData.trade_date, BasicData.close, pre_close_col, BasicData.CE_adjusted_categoty_weighted_free_float_shares]).where(sa.and_(BasicData.code.in_(all_codes), BasicData.trade_date>=index_start_date_dbformat, BasicData.trade_date<=index_end_date_dbformat))
        trade_data = pd.read_sql_query(data_qry, self.env.engine,
                                       parse_dates={BasicData.trade_date.name: db_date_str_format},
                                       index_col=[BasicData.trade_date.name, BasicData.code.name])
        # fixme: 这里的日期比较似乎要好好考虑等号问题
        assert self.trade_calendar_start_date <= index_start_date and index_end_date <= self.trade_calendar_end_date

        trade_dates = pd.date_range(index_start_date, index_end_date, freq=self.trade_date_offset)

        index_codes = form_data[FormDataCol.index_code.value].unique().tolist()
        code_date_dict = {}
        for index_code in index_codes:
            tmp_list = []
            form_data_slc = form_data.loc[form_data[FormDataCol.index_code.value]==index_code, :]
            for trade_date in trade_dates:  # fixme: 可以提升效率
                tmp_bool = (form_data_slc[FormDataCol.start_date.value] <= trade_date) & (form_data_slc[FormDataCol.end_date.value] >= trade_date)
                tmp = form_data_slc.loc[tmp_bool, [FormDataCol.stock_code.value, FormDataCol.weight_factor.value]]
                tmp_list.extend((trade_date, code, weight_factor) for _, code, weight_factor in tmp.itertuples())
            code_date_dict[index_code] = tmp_list


        for index_code in index_codes:
            tmp_date_code = [(trade_date, code) for trade_date, code, weight_factor in code_date_dict[index_code]]
            mutiindex = pd.MultiIndex.from_tuples(tmp_date_code)
            del tmp_date_code
            import gc
            gc.collect()
            index_trade_data = trade_data.loc[mutiindex, :]
            del trade_data
            index_trade_data[FormDataCol.weight_factor.value] = [weight_factor for trade_date, code, weight_factor in  code_date_dict[index_code]]
            # fixme： 没考虑汇率
            index_trade_data["multiplier"] = index_trade_data[BasicData.close.name]*index_trade_data[BasicData.CE_adjusted_categoty_weighted_free_float_shares.name]*index_trade_data[FormDataCol.weight_factor.value]
            index_trade_data["pre_multiplier"] = index_trade_data[pre_close_col.name]*index_trade_data[BasicData.CE_adjusted_categoty_weighted_free_float_shares.name]*index_trade_data[FormDataCol.weight_factor.value]
            net_value_mtp = index_trade_data.loc[:, ["multiplier", "pre_multiplier"]].sum(level=BasicData.trade_date.name)
            net_value = BASE_POINT * net_value_mtp.cumprod()








        self.get_listing_instrument(datetime.datetime(2016, 6, 16))

        self.form_data = form_data

        super(BackTestEngine, self).__init__()

    @cached_property
    def trade_calendar(self):
        query = (sa.select([BasicData.trade_date]).order_by(BasicData.trade_date).distinct(BasicData.trade_date))
        date_str_ser = pd.read_sql_query(query, self.env.engine).squeeze()
        date_time_ser = pd.to_datetime(date_str_ser, format=db_date_str_format)
        return pd.DatetimeIndex(date_time_ser)

    @property
    def natural_calendar_start_date(self):
        """
        自然日历开始时间
        :return:
        """
        return self.trade_calendar[0]

    @property
    def natural_calendar_end_date(self):
        """
        自然日历结束时间
        :return:
        """
        return self.trade_calendar[-1]

    @cached_property
    def instrument_info_df(self):
        query = (sa.select([BasicData.code,
                            func.min(BasicData.trade_date),
                            func.max(BasicData.trade_date)])
                 .group_by(BasicData.code)
                 .order_by(BasicData.code))
        ins_info = pd.read_sql_query(query, self.env.engine)
        ins_info.columns = [InstrumentInfoCol.code.value,
                            InstrumentInfoCol.list_date.value,
                            InstrumentInfoCol.delist_date.value]
        ins_info[InstrumentInfoCol.list_date.value] = pd.to_datetime(ins_info[InstrumentInfoCol.list_date.value],
                                                                     format=db_date_str_format)
        ins_info[InstrumentInfoCol.delist_date.value] = pd.to_datetime(ins_info[InstrumentInfoCol.delist_date.value],
                                                                       format=db_date_str_format)
        return ins_info.set_index(InstrumentInfoCol.code.value)





if __name__ == '__main__':
    import pandas as pd
    data_df = pd.read_excel("result.xlsx")
    data_df[FormDataCol.stock_code.value] = data_df[FormDataCol.stock_code.value].map(lambda x: "%06d" % x)
    data_df.loc[:, FormDataCol.start_date.value] = pd.to_datetime(data_df.loc[:, FormDataCol.start_date.value].map(str))
    data_df.loc[:, FormDataCol.end_date.value] = pd.to_datetime(data_df.loc[:, FormDataCol.end_date.value].map(str))

    en = BackTestEngine(form_data=data_df)
    # en.trade_calendar
    # en.holiday_calendar

    st = pd.Timestamp(2008, 1, 1)
    et = pd.Timestamp(2009, 1, 1)
    en.get_listing_instrument(et)
    en.is_in_trade_calendar_range(st)
    # en.tdate_range(st, et)







