import pandas as pd
from scipy.stats import mannwhitneyu

df = pd.read_csv('input.csv', sep='\t')
df.columns = df.columns.str.strip().str.upper()

comparisons = [
    ('b', 'f'),
    ('a', 'f'),
    ('b', 'd'),
    ('g', 'a'),
    ('f', 'e')
]


#TYPE	DESCRIPTION
# a	base incremental (default=index)
# b	incremental, variable batch update, (default=index)
# c	incremental, fixed batch update, (default=index)
# d	incremental, variable batch update, (noindex)
# e	standard (index)
# f	standard (default = noindex)
# g	base incremental (noindex)








resultados = []

for tipo1, tipo2 in comparisons:
    df_sub = df[df['TIPO'].isin([tipo1, tipo2])]
    for cod_ver in df_sub['COD_VER_EXEC'].unique():
        grupo_1 = df_sub[(df_sub['COD_VER_EXEC'] == cod_ver) & (df_sub['TIPO'] == tipo1)]['TOTAL_TIME']
        grupo_2 = df_sub[(df_sub['COD_VER_EXEC'] == cod_ver) & (df_sub['TIPO'] == tipo2)]['TOTAL_TIME']

        if len(grupo_1) > 0 and len(grupo_2) > 0:
            u, p = mannwhitneyu(grupo_1, grupo_2, alternative='less')  # H₀: tipo1 ≥ tipo2 ; H₁: tipo1 < tipo2
            h0 = 'REJEITA H0' if p < 0.05 else 'NÃO REJEITA H0'

            resultados.append({
                'Comparação': f'{tipo1}>{tipo2}',
                'COD_VER_EXEC': cod_ver,
                'U-Value': u,
                'p-Value': p,
                'Resultado': h0,
                'Mediana_1': grupo_1.median(),
                'Q1_1': grupo_1.quantile(0.25),
                'Q3_1': grupo_1.quantile(0.75),
                'IQR_1': grupo_1.quantile(0.75) - grupo_1.quantile(0.25),
                'Mediana_2': grupo_2.median(),
                'Q1_2': grupo_2.quantile(0.25),
                'Q3_2': grupo_2.quantile(0.75),
                'IQR_2': grupo_2.quantile(0.75) - grupo_2.quantile(0.25),
            })




# Conversion to dataframe
resultado_df = pd.DataFrame(resultados)


with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(resultado_df)


# Salva tudo em CSV separado por ponto e vírgula
resultado_df.to_csv('report_mann_whitney.csv', sep=';', index=False)
