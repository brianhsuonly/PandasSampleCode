import pandas as pd
import numpy as np
import datetime
import os

from datetime import datetime, timedelta


root = 'DBpath'
financialReportPath = os.path.join(root, 'financialReport')
statisticsPath = os.path.join(root, 'statistics')

flag = []
counter = {}


def findHeader(filePath, keyword):
    with open(filePath, 'r') as file:
        lines = file.readlines()
        for i in range(len(lines)):
            line = lines[i].replace('"', '').replace('\n', '')
            words = line.split(',')
            for j in range(len(words)):
                if words[j] == keyword:
                    return i


def loadTransaction(day, type, index=[]):
    if type == 'transaction':
        transactionRecordPath = os.path.join(root, 'stock', day + "-每日股票交易.csv")
        headerRows = findHeader(transactionRecordPath, '證券代號')
        transaction = pd.read_csv(transactionRecordPath, encoding='big5hkscs', header=headerRows,
                                  skip_blank_lines=False, thousands=',')
    else:
        transactionRecordPath = os.path.join(root, 'stock', day + "-每日法人股票交易.csv")
        headerRows = findHeader(transactionRecordPath, '證券代號')
        tail = findHeader(transactionRecordPath, '說明:')
        transaction = pd.read_csv(transactionRecordPath, encoding='big5hkscs', header=headerRows, thousands=',',
                                  nrows=tail - 1 - headerRows)

    transaction = transaction.apply(lambda x: pd.to_numeric(x.replace('--', np.nan), downcast='float', errors='ignore'))
    transaction['證券代號'].replace('=', '', inplace=True, regex=True)
    transaction['證券代號'].replace('"', '', inplace=True, regex=True)

    if index != []:
        transaction.set_index(keys=index, inplace=True)

    return transaction


def date_generator(startDay, endDay, shift=0):
    day = datetime.strptime(startDay, '%Y-%m-%d')
    endDay = datetime.strptime(endDay, '%Y-%m-%d')
    while True:
        daystr = day.strftime('%Y%m%d')
        transactionRecordPath = os.path.join(root, 'stock', daystr + "-每日股票交易.csv")
        if (os.path.isfile(transactionRecordPath) and os.stat(transactionRecordPath).st_size != 0):
            break
        day = day + timedelta(days=1)

    for i in range(shift):
        while True:
            day = day + timedelta(days=1)
            daystr = day.strftime('%Y%m%d')
            transactionRecordPath = os.path.join(root, 'stock', daystr + "-每日股票交易.csv")
            if (os.path.isfile(transactionRecordPath) and os.stat(transactionRecordPath).st_size != 0):
                break
    if shift != 0:
        endDay = endDay + timedelta(days=shift * 3 + 5)
    for single_date in pd.date_range(start=day, end=endDay):
        daystr = single_date.strftime('%Y%m%d')
        transactionRecordPath = os.path.join(root, 'stock', daystr + "-每日股票交易.csv")
        if (os.path.isfile(transactionRecordPath) and os.stat(transactionRecordPath).st_size != 0):
            yield single_date


if __name__ == '__main__':
    record = pd.DataFrame(columns=['Date', '證券代號'])

    periodtransactionRecord = pd.DataFrame()
    start = '2018-01-01'
    end = '2020-10-01'
    deadline = 23

    initial_iter = date_generator(start, end)
    for i in range(deadline):
        day = next(initial_iter)
        transaction = loadTransaction(day.strftime('%Y%m%d'), 'transaction', '證券代號')
        institution = loadTransaction(day.strftime('%Y%m%d'), 'institution', '證券代號')
        institution.drop(['證券名稱'], axis=1, inplace=True)

        dayData = transaction.merge(institution, left_index=True, right_index=True, how='outer')
        dayData.drop(dayData.columns[dayData.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
        dayData['Date'] = day
        dayData = dayData[dayData['成交股數'] > 500000]
        if not '外資買賣超股數' in dayData.columns:
            dayData['外資買賣超股數'] = dayData['外陸資買賣超股數(不含外資自營商)'] + dayData['外資自營商買賣超股數']
        periodtransactionRecord = pd.concat([periodtransactionRecord, dayData], axis=0, sort=False)

    loop_iter = date_generator(start, end, 0)
    shift_iter = date_generator(start, end, deadline + 1)
    for single_date in loop_iter:
        day = single_date.strftime('%Y%m%d')

        for name, group in periodtransactionRecord.groupby(periodtransactionRecord.index):
            if group.shape[0] == 23:
                headcondition = group.iloc[:20, :]
                headcondition = headcondition[headcondition['外資買賣超股數'] < -2000000]
                tailcondition = group.iloc[:-3, :]
                tailcondition = tailcondition[tailcondition['外資買賣超股數'] > 2000000]

                if headcondition.shape[0] >= 14 and tailcondition.shape[0] == 3:
                    row = pd.Series(dtype=float)
                    row['Date'] = single_date
                    row['證券代號'] = name
                    row['證券名稱'] = group.iloc[0]['證券名稱']
                    record = pd.concat([record, row.to_frame().T], axis=0, sort=False, ignore_index=True)

        periodtransactionRecord.query('Date>Date.min()', inplace=True)
        shiftDate = next(shift_iter)
        transaction = loadTransaction(shiftDate.strftime('%Y%m%d'), 'transaction', '證券代號')
        institution = loadTransaction(shiftDate.strftime('%Y%m%d'), 'institution', '證券代號')
        institution.drop(['證券名稱'], axis=1, inplace=True)
        dayData = transaction.merge(institution, left_index=True, right_index=True, how='outer')
        dayData.drop(dayData.columns[dayData.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
        dayData['Date'] = shiftDate
        dayData = dayData[dayData['成交股數'] > 500000]
        if not '外資買賣超股數' in dayData.columns:
            dayData['外資買賣超股數'] = dayData['外陸資買賣超股數(不含外資自營商)'] + dayData['外資自營商買賣超股數']
        periodtransactionRecord = pd.concat([periodtransactionRecord, dayData], axis=0, sort=False)
        periodtransactionRecord.sort_values(by=['Date'], axis=0, inplace=True)

        print(day + 'complete')
    record.drop(dayData.columns[dayData.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
    record.to_csv(os.path.join(statisticsPath, start + '~' + end + ' record.csv'), encoding='big5hkscs', index=False)

