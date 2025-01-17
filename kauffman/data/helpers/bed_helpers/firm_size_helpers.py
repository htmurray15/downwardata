import sys
import requests
import numpy as np
import pandas as pd
import kauffman.constants as c


def _data_lines(table, firm_size):
    url = f'https://www.bls.gov/web/cewbd/f.0{firm_size}.table{table}_d.txt'
    lines = requests.get(url).text.split('\n')
    return lines


def df_create(lines):
    row = []
    year = 1992
    for line in lines[12:-2]:
        line_list = line.split()
        if line_list:
            if line_list[0].isnumeric():
                row.append(line_list)
                year = line_list[0]
            if line_list[0] in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']:
                row.append([year] + line_list)

    df = pd.DataFrame(row, columns=c.table_firm_size_columns).\
        replace(',', '', regex=True).\
        astype({col: 'float' for col in c.table_firm_size_columns[2:]})
    df.iloc[:, 2:9] = df.iloc[:, 2:9] * 1000
    return df


def _firm_size_data_create(table, firm_size):
    return df_create(_data_lines(table, firm_size)).\
        assign(
            size=c.size_dic2[firm_size],
            fips='00',
            region='US',
            quarter=lambda x: x['quarter'].map(c.month_to_quarter_dict)
        ) \
        [['fips', 'region', 'time', 'quarter', 'size'] + c.table_firm_size_columns[2:]]
