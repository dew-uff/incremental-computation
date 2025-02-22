import psycopg2

# Configurações de conexão com o PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "experimento",
    "user": "postgres",
    "password": ""  # Defina a senha se necessário
}

# Template para o comando DROP INDEX
DROP_INDEX_QUERY_TEMPLATE = "DROP INDEX IF EXISTS {index_name};"

def drop_indexes():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Remover índices das tabelas agreginc1 até agreginc8
        for i in range(1, 9):
            index_name = f"idx_agregnorm{i}_compound"  # Nome correto do índice
            drop_query = DROP_INDEX_QUERY_TEMPLATE.format(index_name=index_name)
            print(f"Dropping index {index_name}...")
            cur.execute(drop_query)

        # Commit para aplicar as alterações
        conn.commit()
        print("Indexes dropped successfully.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    drop_indexes()
