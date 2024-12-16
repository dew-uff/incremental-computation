import hashlib


def calcular_hash(filepath):
    """
    Calcula o hash MD5 de um arquivo para verificar sua unicidade.

    Parâmetros:
    filepath (str): Caminho do arquivo a ser verificado.

    Retorna:
    str: Hash MD5 do arquivo.
    """
    with open(filepath, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


# Lista de arquivos a serem verificados (ajustada para 8 arquivos)
arquivos_gerados = [f'saida2_evolnovo{i}.csv' for i in range(1, 9)]  # Range ajustado para 1 a 8

# Calcula os hashes de cada arquivo gerado
hashes = {arquivo: calcular_hash(arquivo) for arquivo in arquivos_gerados}

# Imprime os resultados
print("Hashes dos arquivos gerados:")
for arquivo, hash in hashes.items():
    print(f"{arquivo}: {hash}")
