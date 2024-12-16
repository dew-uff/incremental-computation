import pandas as pd
import numpy as np
from scipy.stats import beta, dirichlet, norm
import openpyxl  # Certifique-se de que openpyxl está instalado


# Função principal para processar os dados e gerar os arquivos
def processar_dados(seed, output_file):
    np.random.seed(seed)

    # Carregar dados
    file_path = 'AGREG.xlsx'  # Ajuste o caminho para o arquivo correto
    df_agreg = pd.read_excel(file_path, dtype={'UF': str, 'UFMUN': str, 'CNAE_DIV': str})

    # Substituir '-' por 0
    df_agreg.replace('-', 0, inplace=True)

    # Separar DataFrames
    df1 = df_agreg[df_agreg.isin(['X']).any(axis=1)]  # Apenas registros com 'X'
    df2 = df_agreg[~df_agreg.isin(['X']).any(axis=1)]  # Apenas registros com valores válidos

    # Função para distribuir valores conforme a distribuição Beta
    def distribute_values(nuls, total_value, alpha=3, beta_val=9):
        if nuls == 0:
            return np.zeros(nuls)
        values = beta.rvs(alpha, beta_val, size=nuls)
        values = values / np.sum(values) * total_value

        # Arredondar valores e ajustar a soma para garantir que o total seja igual ao informado
        values = np.round(values).astype(int)
        adjustment = total_value - np.sum(values)
        while adjustment != 0:
            for i in range(abs(adjustment)):
                index = np.random.randint(0, len(values))
                if adjustment > 0:
                    values[index] += 1
                elif adjustment < 0 and values[index] > 0:
                    values[index] -= 1
            adjustment = total_value - np.sum(values)

        return values

    # Lista para armazenar os dados
    data = []




    # Processar df2
    for idx, row in df2.iterrows():
        nuls = int(row['NULS'])
        poa = int(row['POA'])
        pot = int(row['POT'])
        sal = int(row['SAL'])

        # Distribuir POA
        poa_values = distribute_values(nuls, poa)

        # Distribuir POT usando Dirichlet
        pot_diff = pot - poa
        if pot_diff > 0:
            dirichlet_values = dirichlet.rvs([1] * nuls)[0] * pot_diff
            pot_values = poa_values + np.round(dirichlet_values).astype(int)
        else:
            pot_values = poa_values

        # Distribuir SAL
        sal_values = np.zeros(nuls)
        non_zero_indices = poa_values != 0
        noise = 0.1 * np.random.uniform(-1, 1, size=nuls)
        sal_values[non_zero_indices] = (poa_values[non_zero_indices] / poa) * sal * (1 + noise[non_zero_indices])
        sal_sum = np.sum(sal_values)
        if sal_sum != 0:
            sal_values = sal_values / sal_sum * sal  # Normalizar para garantir que a soma seja igual a SAL
        sal_values = np.round(sal_values).astype(int)

        # Ajustar para garantir que a soma seja igual a SAL
        adjustment = sal - np.sum(sal_values)
        while adjustment != 0:
            for i in range(abs(adjustment)):
                index = np.random.randint(0, len(sal_values))
                if adjustment > 0:
                    sal_values[index] += 1
                elif adjustment < 0 and sal_values[index] > 0:
                    sal_values[index] -= 1
            adjustment = sal - np.sum(sal_values)

        # Adicionar as linhas ao dataset
        for i in range(nuls):
            data.append([row['UF'], row['UFMUN'], row['CNAE_DIV'], poa_values[i], pot_values[i], sal_values[i]])

    # Converter lista para DataFrame
    microdados = pd.DataFrame(data, columns=['UF', 'UFMUN', 'CNAE_DIV', 'POAM', 'POTM', 'SALM'])

    # Adicionar coluna de identificador sequencial
    microdados.insert(0, 'IDENTIFICADOR', pd.Series([f"{i:08d}" for i in range(1, len(microdados) + 1)]))

    # Calcular totalizadores
    total_nuls = len(microdados)
    total_poa = microdados['POAM'].sum()
    total_pot = microdados['POTM'].sum()
    total_sal = microdados['SALM'].sum()

    # Calcular resíduos
    n_residuo = 6321759 - total_nuls
    poa_residuo = 47616457 - total_poa
    pot_residuo = 55296012 - total_pot
    sal_residuo = 1994858026 - total_sal

    # Processar df1 para criar MICRODADOS2
    last_id = int(microdados['IDENTIFICADOR'].iloc[-1]) + 1
    data2 = []

    for idx, row in df1.iterrows():
        nuls = int(row['NULS'])
        for i in range(nuls):
            data2.append([f"{last_id:08d}", row['UF'], row['UFMUN'], row['CNAE_DIV'], 0, 0, 0])
            last_id += 1

    # Converter lista para DataFrame
    microdados2 = pd.DataFrame(data2, columns=['IDENTIFICADOR', 'UF', 'UFMUN', 'CNAE_DIV', 'POAM', 'POTM', 'SALM'])

    # Distribuir os resíduos em MICRODADOS2
    n_residuo2 = len(microdados2)
    noise = norm.rvs(loc=0, scale=0.2, size=n_residuo2)

    microdados2['POAM'] = np.round((poa_residuo / n_residuo2) * (1 + noise)).astype(int)
    microdados2['POTM'] = np.round((poa_residuo / n_residuo2) * (1 + noise)).astype(int) + 1
    microdados2['SALM'] = np.round((sal_residuo / n_residuo2) * (1 + noise)).astype(int)

    # Ajustar POTM para garantir a soma correta
    total_potm2 = microdados2['POTM'].sum()
    pot_residuo2 = pot_residuo - total_potm2

    if pot_residuo2 > 0:
        adjustment_values = np.zeros(n_residuo2)
        indices = np.random.choice(n_residuo2, pot_residuo2, replace=True)
        for idx in indices:
            adjustment_values[idx] += 1
        microdados2['POTM'] += adjustment_values.astype(int)

    # Combinar MICRODADOS e MICRODADOS2
    microdados_final = pd.concat([microdados, microdados2], ignore_index=True)

    # Ordenar por UFMUN e CNAE_DIV
    microdados_final = microdados_final.sort_values(by=['UFMUN', 'CNAE_DIV'])

    # Importar a tabela CNAE.xlsx
    cnae_file_path = 'CNAE.xlsx'  # Ajuste o caminho para o arquivo correto
    df_cnae = pd.read_excel(cnae_file_path, dtype={'COD_CNAE': str})

    # Adicionar a coluna COD_CNAE em microdados_final de forma eficiente
    cnae_div_cache = {}

    def get_random_cnae(cnae_div):
        if cnae_div not in cnae_div_cache:
            valid_cnaes = df_cnae[df_cnae['COD_CNAE'].str.startswith(cnae_div)]['COD_CNAE'].values
            cnae_div_cache[cnae_div] = valid_cnaes
        return np.random.choice(cnae_div_cache[cnae_div])

    microdados_final['COD_CNAE'] = microdados_final['CNAE_DIV'].apply(lambda x: get_random_cnae(str(x).zfill(2)))

    # Importar a tabela pnatjur.xlsx
    pnatjur_file_path = 'pnatjur.xlsx'  # Ajuste o caminho para o arquivo correto
    df_pnatjur = pd.read_excel(pnatjur_file_path, dtype={'UF': str, 'CNAE_DIV': str})

    # Adicionar colunas de data e NATJUR
    microdados_final['DATA_INI'] = pd.Timestamp('2024-01-01 12:00:00')
    microdados_final['DATA_FIM'] = pd.NaT

    # Adicionar coluna de valor aleatório
    microdados_final['ALEAT'] = np.random.rand(len(microdados_final))

    # Ordenar por UF, CNAE_DIV e ALEAT
    microdados_final = microdados_final.sort_values(by=['UF', 'CNAE_DIV', 'ALEAT'])

    # Função para distribuir NATJUR conforme novo formato de pnatjur
    def distribuir_natur(df, pnatjur):
        resultado = []
        for (uf, cnae_div), grupo in df.groupby(['UF', 'CNAE_DIV']):
            if (uf, cnae_div) in pnatjur.set_index(['UF', 'CNAE_DIV']).index:
                proporcoes = pnatjur[(pnatjur['UF'] == uf) & (pnatjur['CNAE_DIV'] == cnae_div)]
                total_linhas = len(grupo)
                natjur1_count = int(proporcoes['PNATJUR1'].values[0] * total_linhas)
                natjur2_count = int(proporcoes['PNATJUR2'].values[0] * total_linhas)
                natjur3_count = int(proporcoes['PNATJUR3'].values[0] * total_linhas)
                natjur = [1] * natjur1_count + [2] * natjur2_count + [3] * natjur3_count
                while len(natjur) < total_linhas:
                    natjur.append(2)  # Preencher com 2 se faltar linhas
                np.random.shuffle(natjur)
                resultado.extend(natjur)
        return resultado

    # Adicionar NATJUR ao DataFrame
    microdados_final['NATJUR'] = distribuir_natur(microdados_final, df_pnatjur)

    # Remover coluna ALEAT
    microdados_final = microdados_final.drop(columns=['ALEAT'])

    # Exportar microdados_final para CSV
    microdados_final.to_csv(output_file, index=False)


# Executar o processo para 8 sementes diferentes
for i in range(1, 9):
    seed = i
    output_file = f'microdadosf_lote{i}.csv'
    processar_dados(seed, output_file)
    print(f"Lote {i} exportado para: {output_file}")

# Uma correção foi necessária para corrigir problemas de exclusão de zeros a esquerda dos códigos CHAR.
# Optou-se por esta solução no lugar de corrigir o próprio script devido ao tempo de geração das bases ser mais longo.

# Configuração do logger
logging.basicConfig(
    filename='log_corrigir_cnae.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def corrigir_cnaes_arquivos(lista_arquivos):
    """
    Corrige CNAEs com menos de 5 dígitos nos arquivos fornecidos e salva versões corrigidas.

    Args:
        lista_arquivos (list): Lista de caminhos dos arquivos de entrada.
    """
    for arquivo in lista_arquivos:
        print(f"Processando arquivo: {arquivo}")
        logging.info(f"Processando arquivo: {arquivo}")

        # Ler o arquivo
        df = pd.read_csv(arquivo, dtype={'COD_CNAE': 'object'})  # Leia CNAE como string
        total_registros = len(df)
        print(f"Total de registros no arquivo: {total_registros}")
        logging.info(f"Total de registros no arquivo: {total_registros}")

        # Identificar CNAEs inválidos
        invalidos = df['COD_CNAE'][df['COD_CNAE'].str.len() < 5]
        total_invalidos = len(invalidos)
        print(f"Total de CNAEs inválidos encontrados: {total_invalidos}")
        logging.warning(f"Total de CNAEs inválidos encontrados: {total_invalidos}")

        if total_invalidos > 0:
            # Corrigir CNAEs adicionando zeros à esquerda
            df['COD_CNAE'] = df['COD_CNAE'].apply(lambda x: x.zfill(5) if len(str(x)) < 5 else x)
            print(f"Correção aplicada: zeros à esquerda para {total_invalidos} CNAEs.")
            logging.info(f"Correção aplicada: zeros à esquerda para {total_invalidos} CNAEs.")
        else:
            print("Nenhuma correção necessária para este arquivo.")
            logging.info("Nenhuma correção necessária para este arquivo.")

        # Salvar o arquivo corrigido
        arquivo_corrigido = arquivo.replace('.csv', '2.csv')
        df.to_csv(arquivo_corrigido, index=False)
        print(f"Arquivo corrigido salvo como: {arquivo_corrigido}")
        logging.info(f"Arquivo corrigido salvo como: {arquivo_corrigido}")

# Lista de arquivos de entrada
arquivos_entrada = [
    'microdadosf_lote1final.csv',
    'microdadosf_lote2final.csv',
    'microdadosf_lote3final.csv',
    'microdadosf_lote4final.csv',
    'microdadosf_lote5final.csv',
    'microdadosf_lote6final.csv',
    'microdadosf_lote7final.csv',
    'microdadosf_lote8final.csv'
]

# Corrigir os arquivos
corrigir_cnaes_arquivos(arquivos_entrada)
