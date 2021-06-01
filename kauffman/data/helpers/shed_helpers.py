import pandas as pd
import kauffman.constants as c
from kauffman.tools.etl import read_zip
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


def _shed_data_create(demographic_lst, series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase).\
                assign(
                    time=year,
                    fips=lambda x: x['ppstaten'].map(c.shed_state_codes).map(c.state_abb_fips_dic),
                    region=lambda x: x['ppstaten'].map(c.shed_state_codes).map(c.state_abb_name_dic),
                    e2=lambda x: x['e2'].map({-1: "refused", 0: "no", 1: "yes"}),
                    b2=lambda x: x['b2'].map({-1: "refused", 1: "Finding it difficult to get by", 2: "Just getting by", 3: "Doing ok", 4: "Living comfortably"}),
                    ppethm=lambda x: x['ppethm'].map({1: "White, Non‐Hispanic", 2: "Black, Non‐Hispanic", 3: "Other, Non‐Hispanic", 4: "Hispanic", 5: "2+ Races, Non‐Hispanic"}),
                    ppgender=lambda x: x['ppgender'].map({1: "Male", 2: "Female"})
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age", "ppgender": "gender", "b2": "man_financially"}) \
            [['fips', 'region', 'time',] + demographic_lst + series_lst]
            for year in range(2013, 2021)
        ]
    )

if __name__ == '__main__':
     df = _shed_data_create()
     print(df.head())