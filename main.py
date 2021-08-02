#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
    Created by Klaus Lee on 2021/8/1
-------------------------------------------------
"""

import time
import numpy as np
import pandas as pd
import tushare as ts
import datetime
import os

ROOT_PATH = os.path.dirname(__file__)
DATA_PATH = os.path.join(ROOT_PATH, 'Data')
RESULT_PATH = os.path.join(ROOT_PATH, 'Result')
pro = ts.pro_api("6144f3417ac5da6235442a7bafe9ba6931c3fba8dcdd8946d089f862")


def get_trade_cal():
    trade_cal = pro.query('trade_cal', exchange='',
                          fields='exchange,cal_date,is_open,pretrade_date')
    trade_cal.to_csv(os.path.join(DATA_PATH, 'trade_cal.csv'), index=False)


def get_stock_basic():
    stock_basic = pro.query('stock_basic', exchange='', list_status='L',
                            fields='ts_code,symbol,name,area,industry,list_date')
    stock_basic.to_csv(os.path.join(DATA_PATH, 'stock_basic.csv'), index=False)


def read_csv_v1(path, file_name, dedicated_filter=0, **kwargs):
    read_conds = {
        'encoding': 'utf-8',
        'engine': 'python',
        'index_col': None,
        'skiprows': None,
        'na_values': np.nan,
        **kwargs
    }
    data = pd.read_csv(os.path.join(path, file_name), **read_conds)
    # 去重
    data.drop_duplicates(inplace=True)
    # 专门化清洗
    if dedicated_filter == 1:
        # 日期格式化
        data['cal_date'] = pd.to_datetime(data['cal_date'], format='%Y%m%d')
        data['pretrade_date'] = pd.to_datetime(data['pretrade_date'], format='%Y%m%d')
        # 按日期由近到远排序
        data.sort_values(by='cal_date', ascending=False, inplace=True)
        # 重置index
        data.reset_index(inplace=True, drop=True)
    elif dedicated_filter == 2:
        pass
    else:
        pass
    return data


class QIFinal:
    def __init__(self, trade_cal, ts_code, end_date=datetime.datetime.today().date(), date_delta=20):
        self.trade_cal = trade_cal
        self.ts_code = ts_code
        self.end_date = end_date
        self.date_delta = date_delta
        self.end_date_real = self.get_end_date_real()
        self.start_date = self.get_start_date()
        self.start_df, self.end_df = self.get_data()

    def get_end_date_real(self):
        # 若当日非交易日或未收盘，则先校准到上一交易日
        is_end_date_open = self.trade_cal.loc[self.trade_cal['cal_date'].dt.date == self.end_date, ['is_open']].to_numpy()[0][0]
        # TODO:判断时间的逻辑默认为观测日为今天，该逻辑可以扩写为任意历史日且不影响后边的程序
        if is_end_date_open == 1 and datetime.datetime.now().hour >= 15:
            end_date_real = self.end_date
        else:
            end_date_real = self.trade_cal.loc[self.trade_cal['cal_date'].dt.date == self.end_date, :]['pretrade_date'].to_numpy(
                datetime.date)[0].date()
        return end_date_real

    def get_start_date(self):
        end_date_loc = self.trade_cal.loc[self.trade_cal['cal_date'].dt.date == self.end_date_real, :].index[0]
        i = 1
        if self.end_date == self.end_date_real:
            delta = 0
        else:
            delta = -1
        while i <= self.date_delta:
            delta += 1
            is_cache_open = self.trade_cal.loc[end_date_loc + delta, 'is_open']
            if is_cache_open == 1:
                i += 1
        start_date = self.trade_cal.loc[end_date_loc + delta, 'cal_date'].date()
        return start_date

    def get_data(self):
        def get_daily(ts_code, trade_date, retry=3):
            print('获取{1}当日的{0}数据，尝试第{2}次'.format(ts_code, trade_date, retry))
            for _ in range(retry):
                try:
                    df = pro.query('daily', ts_code=ts_code, trade_date=trade_date)
                except:
                    time.sleep(0.5)
                    print('ERROR')
                else:
                    time.sleep(0.1)
                    return df
        try:
            print('尝试直接方案')
            start_df = pro.query('daily', trade_date=datetime.datetime.strftime(self.start_date, '%Y%m%d'))
            end_df = pro.query('daily', trade_date=datetime.datetime.strftime(self.end_date_real, '%Y%m%d'))
        except:
            print('尝试备选方案（极慢）')
            start_list = [get_daily(i, datetime.datetime.strftime(self.start_date, '%Y%m%d')) for i in self.ts_code]
            end_list = [get_daily(i, datetime.datetime.strftime(self.end_date_real, '%Y%m%d')) for i in self.ts_code]
            start_df = pd.DataFrame(start_list)
            end_df = pd.DataFrame(end_list)
        else:
            print('直接方案成功')
        start_df.to_csv(os.path.join(RESULT_PATH, 'start_df.csv'), index=False)
        end_df.to_csv(os.path.join(RESULT_PATH, 'end_df.csv'), index=False)
        return start_df, end_df

    def result(self):
        start_df = read_csv_v1(RESULT_PATH, 'start_df.csv')
        end_df = read_csv_v1(RESULT_PATH, 'end_df.csv')
        start_df_lite = start_df.loc[:, ['ts_code', 'close']]
        start_df_lite.columns = ['ts_code', 'close_{0}'.format(datetime.datetime.strftime(self.start_date, '%Y%m%d'))]
        end_df_lite = end_df.loc[:, ['ts_code', 'close']]
        end_df_lite.columns = ['ts_code', 'close_{0}'.format(datetime.datetime.strftime(self.end_date_real, '%Y%m%d'))]
        # 交集
        result_df_cache = pd.merge(start_df_lite, end_df_lite, how='inner', on=['ts_code'])
        result_df_cache['rate'] = result_df_cache['close_{0}'.format(datetime.datetime.strftime(self.end_date_real, '%Y%m%d'))] / \
                                  result_df_cache['close_{0}'.format(datetime.datetime.strftime(self.start_date, '%Y%m%d'))] - 1
        result_df_cache.to_csv(os.path.join(RESULT_PATH, 'result_df_cache.csv'), index=False)
        # 按日期由近到远排序
        result_df = result_df_cache.sort_values(by='rate', ascending=True, inplace=False)
        # 重置index
        result_df.reset_index(inplace=True, drop=True)
        result_df = result_df.loc[result_df['rate'] <= -0.2]
        result_df.to_csv(os.path.join(RESULT_PATH, 'result_df.csv'), index=False)
        print('筛选结果见Result文件夹下Result_df.csv')
        return result_df


if __name__ == '__main__':
    # 获取交易日历，这个函数只在数据缺省时执行
    if os.path.exists((os.path.join(DATA_PATH, 'trade_cal.csv'))):
        print('检测到已存在交易日历文件')
    else:
        print('获取交易日历文件')
        get_trade_cal()
    # 获取基础信息，这个函数只在数据缺省时执行
    if os.path.exists((os.path.join(DATA_PATH, 'stock_basic.csv'))):
        print('检测到已存在基础信息文件')
    else:
        print('获取基础信息文件')
        get_stock_basic()
    df_trade_cal = read_csv_v1(DATA_PATH, 'trade_cal.csv', dedicated_filter=1)
    df_stock_basic = read_csv_v1(DATA_PATH, 'stock_basic.csv', dedicated_filter=0)
    ts_code_array = df_stock_basic.loc[:, 'ts_code'].to_numpy()
    a = QIFinal(trade_cal=df_trade_cal, ts_code=ts_code_array)
    a.result()
