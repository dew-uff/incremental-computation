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


def limpar_tabelas(exec="n"):
    """
    Limpa os dados das tabelas AgregNorm e, opcionalmente, ExecContrNorm.

    Parâmetros:
        exec (str): Define se a tabela ExecContrNorm também será limpa.
                    "s" para sim, "n" (default) para não.
    """
    try:
        # Conexão com o banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Limpar AgregNorm (sempre executa)
        logging.info("Limpando dados da tabela AgregNorm...")
        cursor.execute("TRUNCATE TABLE AgregNorm RESTART IDENTITY CASCADE;")
        logging.info("Tabela AgregNorm limpa com sucesso.")
        print("Tabela AgregNorm limpa com sucesso.")

        # Opcionalmente limpar ExecContrNorm
        if exec.lower() == "s":
            logging.info("Limpando dados da tabela ExecContrNorm...")
            cursor.execute("TRUNCATE TABLE ExecContrNorm RESTART IDENTITY CASCADE;")

            logging.info("Tabela ExecContrNorm limpa com sucesso.")
            print("Tabela ExecContrNorm limpa com sucesso.")
        else:
            logging.info("A tabela ExecContrNorm não foi limpa (exec=n).")
            print("A tabela ExecContrNorm não foi limpa.")

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
    print("Escolha a limpeza das tabelas:")
    print("Por padrão, apenas AgregNorm será limpa.")
    exec_choice = input("Deseja também limpar ExecContrNorm? (s/n): ").strip().lower()

    limpar_tabelas(exec=exec_choice)
