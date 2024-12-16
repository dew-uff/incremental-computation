import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging

# Configuração do logger para registrar em um arquivo
logging.basicConfig(filename='exec_saida_final_inc.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def print_tempo_execucao(start_time, message):
    """
    Calcula e registra o tempo de execução de uma operação.
    """
    end_time = time.time()
    execution_time = end_time - start_time
    log_message = f"{message} - Tempo de execução: {execution_time:.4f} segundos"
    print(log_message)
    logging.info(log_message)

def mix(df, s):
    """
    Embaralha as linhas de um DataFrame com base em uma semente aleatória.
    """
    start_time = time.time()
    np.random.seed(s)
    df['x'] = np.random.rand(len(df))
    df = df.sort_values(by='x').drop(columns='x').reset_index(drop=True)
    print_tempo_execucao(start_time, "mix(df, s) concluído")
    return df

def distribuir_intervalo(df, col, valores, incremento=1):
    """
    Distribui valores uniformemente em uma coluna com incrementos.
    """
    intervalos = np.array_split(df.index, len(valores))
    for i, intervalo in enumerate(intervalos):
        df.loc[intervalo, col] = valores[i] + timedelta(seconds=incremento)

def processar_arquivo(input_file, output_file, seed, datalist, a, b, c, INC):
    """
    Processa um arquivo CSV aplicando alterações específicas de acordo com as proporções definidas.
    """
    if not (0 <= a <= 1 and 0 <= b <= 1 and 0 <= c <= 1 and abs(a + b + c - 1) < 1e-6):
        raise ValueError("As proporções de exclusões, inclusões e atualizações devem somar 1.")

    if not (0 <= INC <= 100):
        raise ValueError("A variável INC deve ser uma porcentagem entre 0 e 100")

    logging.info(f"Iniciando o processamento do arquivo: {input_file} com semente {seed}")

    # Conversão de datalist para objetos datetime
    datalist = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for d in datalist]

    # Leitura do CSV
    start_time = time.time()
    dtypes = {
        'COD_SEQ': 'int64',          # Identificador numérico
        'COD_VERSAO_ESTAB': 'int64', # Identificador numérico
        'UFMUN': 'object',           # Codificação hierárquica, deve ser string
        'COD_CNAE': 'object',        # Codificação hierárquica, deve ser string
        'NATJUR': 'object',          # Codificação categórica, deve ser string
        'POAM': 'int64',             # Valor numérico
        'POTM': 'int64',             # Valor numérico
        'SALM': 'float64'            # Valor numérico
    }
    df = pd.read_csv(input_file, dtype=dtypes, parse_dates=['DATA_INI', 'DATA_FIM'])
    print_tempo_execucao(start_time, "Leitura do CSV concluída")

    # Garantir que UFMUN, COD_CNAE e NATJUR estão no formato correto
    df['UFMUN'] = df['UFMUN'].apply(lambda x: x.zfill(7) if pd.notna(x) else x)  # UFMUN deve ter 7 caracteres
    df['COD_CNAE'] = df['COD_CNAE'].apply(lambda x: x.zfill(5) if pd.notna(x) else x)  # COD_CNAE deve ter 5 caracteres
    df['NATJUR'] = df['NATJUR'].apply(lambda x: x.zfill(1) if pd.notna(x) else x)  # NATJUR deve ter 1 caractere

    # Renomear e reorganizar colunas
    df = df[['COD_SEQ', 'COD_VERSAO_ESTAB', 'UFMUN', 'COD_CNAE', 'NATJUR', 'POAM', 'POTM', 'SALM', 'DATA_INI', 'DATA_FIM']]
    df.columns = ['SEQ', 'CODVER', 'UFMUN', 'CNAE', 'NATJUR', 'POAM', 'POTM', 'SALM', 'DINI', 'DFIM']
    print_tempo_execucao(start_time, "Renomeação e reordenação de colunas concluídas")

    # Definição das variáveis baseadas em proporções
    Z0 = df['SEQ'].max()
    P1 = int(Z0 * (INC / 100) * a)
    P2 = int(Z0 * INC / 100)
    print_tempo_execucao(start_time, "Definição de variáveis concluída")

    # Embaralhar o DataFrame
    df = mix(df, seed)

    # Distribuir valores de DFIM
    distribuir_intervalo(df.loc[:P1 - 1], 'DFIM', datalist, incremento=0)
    print_tempo_execucao(start_time, "Distribuição de valores para DFIM concluída")

    # Separar registros para atualizações e inclusões
    start = P1 + 1
    end = P2
    df1 = df.iloc[start:end].copy()
    Z1 = len(df1)
    x = int((b / (b + c)) * Z1)  # Division point between updates and inclusions

    # Atualizações
    df1.loc[df1.index[:x], 'CODVER'] = 2
    distribuir_intervalo(df1.iloc[:x], 'DINI', datalist, incremento=1)

    # Inclusões
    max_seq = df['SEQ'].max()
    novos_seq = range(max_seq + 1, max_seq + 1 + (len(df1) - x))
    df1.loc[df1.index[x:], 'SEQ'] = novos_seq
    distribuir_intervalo(df1.iloc[x:], 'DINI', datalist, incremento=1)

    # Adicionar df1 ao DataFrame principal
    df = pd.concat([df, df1], ignore_index=True)
    print_tempo_execucao(start_time, "Concatenação de df1 em df concluída")

    # Ordenar por SEQ e CODVER
    df = df.sort_values(by=['SEQ', 'CODVER']).reset_index(drop=True)

    # Ajustar DFIM com base em condições
    mask = (
        (df['SEQ'] == df['SEQ'].shift(-1)) &
        (df['CODVER'] == 1) &
        (df['CODVER'].shift(-1) == 2)
    )
    df.loc[mask, 'DFIM'] = df['DINI'].shift(-1)[mask] - timedelta(seconds=1)

    # Salvar no arquivo de saída
    df.to_csv(output_file, index=False, float_format='%.0f', date_format='%Y-%m-%d %H:%M:%S')
    print_tempo_execucao(start_time, f"Gravação do arquivo {output_file} concluída")
    logging.info(f"Processamento do arquivo {input_file} concluído com sucesso.")

# Loop para processar arquivos
input_files = [f'microdadosf_lote{i}final2.csv' for i in range(1, 9)]
output_files = [f'saida2_evolnovo{i}.csv' for i in range(1, 9)]
seeds = [3, 5, 7, 11, 13, 17, 19, 23]
datalist = [
    "2024-01-04 23:59:59", "2024-01-08 23:59:59", "2024-01-12 23:59:59",
    "2024-01-16 23:59:59", "2024-01-20 23:59:59", "2024-01-24 23:59:59",
    "2024-01-28 23:59:59", "2024-02-01 23:59:59", "2024-02-05 23:59:59",
    "2024-02-09 23:59:59", "2024-02-13 23:59:59", "2024-02-17 23:59:59",
    "2024-02-21 23:59:59", "2024-02-25 23:59:59", "2024-02-29 23:59:59",
    "2024-03-04 23:59:59", "2024-03-08 23:59:59", "2024-03-12 23:59:59",
    "2024-03-16 23:59:59", "2024-03-20 23:59:59"
]

a, b, c = 0.1, 0.6, 0.3
INC = 20

for input_file, output_file, seed in zip(input_files, output_files, seeds):
    processar_arquivo(input_file, output_file, seed, datalist, a, b, c, INC)

print("Processamento de todos os arquivos concluído.")
