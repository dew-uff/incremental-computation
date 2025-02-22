import psycopg2
import logging

# Configuração do logger
logging.basicConfig(
    filename='../cleanup_inc.log',
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

def limpar_tabelas_inc():
    """
    Limpa os dados das tabelas ExecContrInc1-8 e AgregInc1-8.
    """
    try:
        # Conexão com o banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Limpar ExecContrInc1-8 e AgregInc1-8
        for i in range(1, 9):
            # Limpar AgregInc{i}
            tabela_agreginc = f"AgregInc{i}"
            logging.info(f"Limpando dados da tabela {tabela_agreginc}...")
            cursor.execute(f"TRUNCATE TABLE {tabela_agreginc} RESTART IDENTITY CASCADE;")
            logging.info(f"Tabela {tabela_agreginc} limpa com sucesso.")
            print(f"Tabela {tabela_agreginc} limpa com sucesso.")

            # Limpar ExecContrInc{i}
            tabela_execcontrinc = f"ExecContrInc{i}"
            logging.info(f"Limpando dados da tabela {tabela_execcontrinc}...")
            cursor.execute(f"TRUNCATE TABLE {tabela_execcontrinc} RESTART IDENTITY CASCADE;")
            logging.info(f"Tabela {tabela_execcontrinc} limpa com sucesso.")
            print(f"Tabela {tabela_execcontrinc} limpa com sucesso.")

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
    print("Este script limpará as tabelas ExecContrInc1-8 e AgregInc1-8.")
    confirmacao = input("Deseja continuar? (s/n): ").strip().lower()

    if confirmacao == 's':
        limpar_tabelas_inc()
    else:
        print("Operação cancelada.")
