from context import kauffman

from kauffman.data import bfs, bds, pep, bed
from kauffman.tools import alpha, log_log_plot

def data_fetch():
    df = bed('1bf', obs_level=['AL', 'US', 'MO'])
    # df = kauffman.bed('1bf', obs_level=['AL', 'US', 'MO'])

    # df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['AZ'])
    # df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
    # df = kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
    # df = kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)

    # df = kauffman.bds(['FIRM', 'ESTAB'], obs_level='all')

    # df = kauffman.pep(obs_level='us')
    # df = kauffman.pep(obs_level='state')

    #test = kauffman.bfs(['BA_BA'], obs_level='all', seasonally_adj=True, annualize=False, march_shift=True)
    #print(test) should test whether march shift alone anualizes


    # print(df)
    print(df.info())
    print(df.head())
    print(df.tail())

def data_tools():
    bfs(['BA_BA'], obs_level='us'). \
        query('BA_BA == BA_BA'). \
        sort_values('BA_BA', ascending=False). \
        reset_index(drop=True). \
        assign(p=lambda x: (x.index + 1) / x.shape[0]). \
        pipe(log_log_plot, 'BA_BA', 'Business Applications', threshold=5.375)

    df = bfs(['BA_BA'], obs_level='us'). \
        query('BA_BA == BA_BA'). \
        sort_values('BA_BA', ascending=False). \
        reset_index(drop=True). \
        assign(p=lambda x: (x.index + 1) / x.shape[0])

    a = alpha(df, 'BA_BA', threshold=5.375)
    print(a)

if __name__ == '__main__':
    # data_fetch()
    data_tools()