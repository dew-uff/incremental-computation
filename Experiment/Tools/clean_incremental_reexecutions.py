import psycopg2

# Configurações do banco de dados
DB_CONFIG = {
    "host": "localhost",
    "database": "experimento",
    "user": "postgres",
    "password": ""
}

# Definindo as tabelas a serem limpas
control_tables = [f"execcontrinc{i}" for i in range(1, 9)]
agreg_tables = [f"agreginc{i}" for i in range(1, 9)]


# Conectar ao banco de dados
def connect_to_db():
    return psycopg2.connect(**DB_CONFIG)


# Função para limpar as tabelas
def clear_tables(control_tables, agreg_tables):
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:

                # ✅ Limpeza das tabelas de agregação primeiro
                for table in agreg_tables:
                    print(f"Limpando tabela {table}...")
                    cursor.execute(f"DELETE FROM {table} WHERE cod_ver_exec != 1;")
                    print(f"Tabela {table} limpa com sucesso!")

                # ✅ Limpeza das tabelas de controle depois
                for table in control_tables:
                    print(f"Limpando tabela {table}...")
                    cursor.execute(f"DELETE FROM {table} WHERE cod_ver_exec != 1;")
                    print(f"Tabela {table} limpa com sucesso!")

                # Confirmar as alterações
                conn.commit()
                print("Todas as tabelas foram limpas com sucesso!")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao limpar as tabelas: {e}")


# Executar a limpeza
if __name__ == "__main__":
    clear_tables(control_tables, agreg_tables)
