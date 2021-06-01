#from context import kauffman

import sys
import pandas as pd
from kauffman.data import acs, bfs, bds, pep, bed, qwi, shed
from kauffman.tools import alpha, log_log_plot, maximum_to_sum_plot, excess_conditional_expectation, \
    maximum_quartic_variation
from kauffman.tools.etl import county_msa_cross_walk as cw, read_zip


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 40000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)



def _data_fetch():

    df = shed(['gender'], ['med_exp_12_months'])
    print(df.head())
    sys.exit()

if __name__ == '__main__':
     _data_fetch()
    # _distribution_tests()
    # _etl_tests()
