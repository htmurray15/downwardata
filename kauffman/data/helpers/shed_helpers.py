import pandas as pd
import kauffman.constants as c
from kauffman.tools.etl import read_zip
from kauffman.data.eship_data_sources import pep
import sys

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
# pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None



def _col_names_lowercase(df):
    df.columns = df.columns.str.lower()
    return df

# issues: 1) went backwards and lost pep import 2) append doesn't like the df returned from _shed_2014 3)

def _shed_2014(series_lst):
    df = pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    region=lambda x: x['ppstaten'].map(c.shed_state_codes),
                    # region=lambda x: x['upper'].map(c.state_abb_name_dic),
                    fips=lambda x: x['region'].map(c.state_abb_fips_dic),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight3b": "pop_weight"}) \
            for year in range(2014, 2015)
        ]
    )
    pop_2014 = int(pep(obs_level='us').query('time == 2014')['population'])
    df['pop_weight'] = df['weight3'] / df['weight3'].sum() * pop_2014
    df = df[['pop_weight', 'fips', 'region', 'time', ] + series_lst]
    return df

def _shed_2015_2017(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    upper=lambda x: x['ppstaten'].apply(lambda x: x.upper()),
                    region=lambda x: x['upper'].map(c.state_abb_name_dic),
                    fips=lambda x: x['upper'].map(c.state_abb_fips_dic),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight3b": "pop_weight"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2015, 2018)
        ]
    )


def _shed_2018(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    region=lambda x: x['ppstaten'].map(c.state_abb_name_dic),
                    fips=lambda x: x['region'].map(c.state_abb_fips_dic),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight2b": "pop_weight"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2018, 2019)
        ]
    )


def _shed_2019_2020(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    region=lambda x: x['ppstaten'].map(c.state_abb_name_dic),
                    fips=lambda x: x['region'].map(c.state_abb_fips_dic),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight_pop": "pop_weight"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2019, 2021)
        ]
    )

def _shed_binary_weighter(series_lst):
    for x in series_lst:


# def _shed_data_create(obs_level, series_lst, strata):
def _shed_data_create(obs_level, series_lst):
    df = _shed_2014(series_lst).\
        append_shed_2015_2017(series_lst).\
        append(_shed_2018(series_lst), ignore_index=True).\
        append(_shed_2019_2020(series_lst),ignore_index=True)
    df = _shed_binary_weighter(series_lst)
    if obs_level == 'us':
        return df.groupby(['time']).mean()
    # to do - change all categorical variables to 0-1
    # to do - add other aggregating functions? e.g. sum, mean
    elif obs_level == 'individual':
        return df


# _shed_data_create(demographic_lst, series_lst)


# def _shed_data_create(demographic_lst, series_lst):
#     return pd.concat(
#         [
#             read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
#                 pipe(_col_names_lowercase).\
#                 assign(
#                     time=year,
#                     fips=lambda x: x['ppstaten'].map(c.shed_state_codes).map(c.state_abb_fips_dic),
#                     region=lambda x: x['ppstaten'].map(c.shed_state_codes).map(c.state_abb_name_dic),
#                     e2=lambda x: x['e2'].map({-1: "refused", 0: "no", 1: "yes"}),
#                     b2=lambda x: x['b2'].map({-1: "refused", 1: "Finding it difficult to get by", 2: "Just getting by", 3: "Doing ok", 4: "Living comfortably"}),
#                     ppethm=lambda x: x['ppethm'].map({1: "White, Non‐Hispanic", 2: "Black, Non‐Hispanic", 3: "Other, Non‐Hispanic", 4: "Hispanic", 5: "2+ Races, Non‐Hispanic"}),
#                     ppgender=lambda x: x['ppgender'].map({1: "Male", 2: "Female", 'Male': "Male", "Female": "Female"})
#                 ). \
#                 rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age", "ppgender": "gender", "b2": "man_financially"}) \
#             [['fips', 'region', 'time',] + demographic_lst + series_lst]
#             for year in range(2013, 2021)
#         ]
#     )
#
