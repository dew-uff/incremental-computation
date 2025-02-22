import psycopg2
import logging

# Configuração do logger
logging.basicConfig(
    filename='../cleanup.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuração do banco de dados PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "experimento",
    "user": "postgres",
    "password": ""  # Sem senha
}

def limpar_tabelas():
    """
    Limpa os dados das tabelas ExecContrNorm1-8 e AgregNorm1-8.
    As tabelas EstabInc1-8 não serão modificadas.
    """
    try:
        # Conexão com o banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Limpar ExecContrNorm1-8 e AgregNorm1-8
        for i in range(1, 9):
            # Limpar AgregNorm{i}
            tabela_agregnorm = f"AgregNorm{i}"
            logging.info(f"Limpando dados da tabela {tabela_agregnorm}...")
            cursor.execute(f"TRUNCATE TABLE {tabela_agregnorm} RESTART IDENTITY CASCADE;")
            logging.info(f"Tabela {tabela_agregnorm} limpa com sucesso.")
            print(f"Tabela {tabela_agregnorm} limpa com sucesso.")

            # Limpar ExecContrNorm{i}
            tabela_execcontrnorm = f"ExecContrNorm{i}"
            logging.info(f"Limpando dados da tabela {tabela_execcontrnorm}...")
            cursor.execute(f"TRUNCATE TABLE {tabela_execcontrnorm} RESTART IDENTITY CASCADE;")
            logging.info(f"Tabela {tabela_execcontrnorm} limpa com sucesso.")
            print(f"Tabela {tabela_execcontrnorm} limpa com sucesso.")

        # Confirmar as alterações
        conn.commit()
        logging.info("Limpeza de tabelas concluída com sucesso.")
        print("Limpeza concluída.")

    except Exception as e:
        logging.error(f"Erro ao limpar tabelas: {e}")
        print(f"Erro ao limpar tabelas: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("Conexão com o banco encerrada.")


# Teste da função
if __name__ == "__main__":
    print("Este script limpará as tabelas ExecContrNorm1-8 e AgregNorm1-8.")
    print("As tabelas EstabInc1-8 permanecerão inalteradas.")
    confirmacao = input("Deseja continuar? (s/n): ").strip().lower()

    if confirmacao == 's':
        limpar_tabelas()
    else:
        print("Operação cancelada.")
