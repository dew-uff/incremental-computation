import psycopg2

# Configurações de conexão com o PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "experimento",
    "user": "postgres",
    "password": ""  # Defina a senha se necessário
}

# Template SQL para criação do índice
INDEX_QUERY_TEMPLATE = """
    CREATE INDEX IF NOT EXISTS {index_name} 
    ON public.{table_name} 
    USING btree (cod_cube, regiao, cnae, fxpot, natjur, cod_ver_exec DESC);
"""

def create_indexes():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Criar índices para as tabelas agreginc1 até agreginc8
        for i in range(1, 9):
            table_name = f"agreginc{i}"
            index_name = f"idx_agreginc{i}_compound"  # Nome do índice correto
            index_query = INDEX_QUERY_TEMPLATE.format(index_name=index_name, table_name=table_name)
            print(f"Creating index {index_name} on {table_name}...")
            cur.execute(index_query)

        # Commit para salvar as mudanças
        conn.commit()
        print("Indexes created successfully.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_indexes()
