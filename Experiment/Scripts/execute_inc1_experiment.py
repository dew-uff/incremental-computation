# Scripts run experiment inc1, fixed batch (1%), step by step,
# for the 20 partial reexecutions following the first entire execution.

# Imports de bibliotecas

import psycopg2
import time  # Para medir o tempo de execução
from datetime import datetime
import csv
import os


# PARAMETROS:

# Configuração do banco de dados

DB_CONFIG = {
    "host": "localhost",
    "database": "experimento",
    "user": "postgres",
    "password": ""
}

# Lista de datas

datas_execucao = [
        "2024-01-07 00:00",
        "2024-01-11 00:00",
        "2024-01-15 00:00",
        "2024-01-19 00:00",
        "2024-01-23 00:00",
        "2024-01-27 00:00",
        "2024-01-31 00:00",
        "2024-02-04 00:00",
        "2024-02-08 00:00",
        "2024-02-12 00:00",
        "2024-02-16 00:00",
        "2024-02-20 00:00",
        "2024-02-24 00:00",
        "2024-02-28 00:00",
        "2024-03-03 00:00",
        "2024-03-07 00:00",
        "2024-03-11 00:00",
        "2024-03-15 00:00",
        "2024-03-19 00:00",
        "2024-03-23 00:00",
    ]


# Lista de tabelas

tables = [
    ("estabinc1", "execcontrinc1", "agreginc1"),
    ("estabinc2", "execcontrinc2", "agreginc2"),
    ("estabinc3", "execcontrinc3", "agreginc3"),
    ("estabinc4", "execcontrinc4", "agreginc4"),
    ("estabinc5", "execcontrinc5", "agreginc5"),
    ("estabinc6", "execcontrinc6", "agreginc6"),
    ("estabinc7", "execcontrinc7", "agreginc7"),
    ("estabinc8", "execcontrinc8", "agreginc8"),
]

# Conectar ao banco de dados
def connect_to_db():
    return psycopg2.connect(**DB_CONFIG)

# Registrar o início da execução no control_table
def start_execution(data_exec, control_table):
    """
    Registra o início de uma execução incremental na tabela de controle fornecida.
    Captura a última data_exec registrada para ser usada como data_exec_ant.

    :param data_exec: Data de execução atual
    :param control_table: Nome da tabela de controle (Ex: ExecContrNorm1)
    :return: (cod_ver_exec, time_start, data_exec_ant) - Código da execução, timestamp de início e a última data_exec registrada
    """
    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            # Capturar a última data de execução
            cursor.execute(f"""
                        SELECT DATA_EXEC 
                        FROM {control_table} 
                        WHERE COD_VER_EXEC = (SELECT MAX(COD_VER_EXEC) FROM {control_table})
                    """)
            result = cursor.fetchone()

            # Corrigir a lógica de interrupção em caso de ausência de data anterior
            if result is None or result[0] is None:
                raise Exception(f"Erro: Nenhuma versão anterior encontrada em {control_table}. Execução interrompida.")

            # Atribuir o valor correto de data_exec_ant
            data_exec_ant = result[0]

            # Inserir nova linha no controle com a nova data_exec
            time_start = datetime.now()
            cursor.execute(f"""
                INSERT INTO {control_table} (COD_VER_EXEC, DATA_EXEC, TIME_START, TIME_END, COD_RESULT)
                VALUES (
                    (SELECT COALESCE(MAX(COD_VER_EXEC), 0) + 1 FROM {control_table}),
                    %s, %s, NULL, '1'
                )
                RETURNING COD_VER_EXEC;
            """, (data_exec, time_start))
            cod_ver_exec = cursor.fetchone()[0]
            conn.commit()
            print(
                f"Execução iniciada na tabela {control_table}: COD_VER_EXEC={cod_ver_exec}, TIME_START={time_start}, DATA_EXEC_ANT={data_exec_ant}")
            return cod_ver_exec, time_start, data_exec_ant


# Finalizar a execução no control_table



def end_execution(cod_ver_exec, time_start, control_table, agreg_table):
    """
    Finaliza a execução registrando tempo, tuplas e valores.

    :param cod_ver_exec: Código da execução iniciada
    :param time_start: Timestamp de início
    :param control_table: Nome da tabela de controle (Ex: ExecContrNorm1)
    :param agreg_table: Nome da tabela de agregação (Ex: AgregNorm1)
    """
    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            time_end = datetime.now()
            try:
                # Calcular valores
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {agreg_table} WHERE COD_VER_EXEC = %s;
                """, (cod_ver_exec,))
                n_tuples = cursor.fetchone()[0]
                n_values = n_tuples * 31  # Ajuste conforme necessário

                # Calcular duração da execução como intervalo
                time_exec_seconds = (time_end - time_start).total_seconds()
                time_exec_interval = f"{int(time_exec_seconds // 3600):02}:{int((time_exec_seconds % 3600) // 60):02}:{int(time_exec_seconds % 60):02}"  # Formato HH:MM:SS

                # Atualizar registro na tabela de controle
                cursor.execute(f"""
                    UPDATE {control_table}
                    SET TIME_END = %s, N_TUPLE = %s, N_VALUES = %s, TIME_EXEC = %s::INTERVAL, COD_RESULT = 0
                    WHERE COD_VER_EXEC = %s;
                """, (time_end, n_tuples, n_values, time_exec_interval, cod_ver_exec))
                conn.commit()
                print(f"Execução finalizada na tabela {control_table}: COD_VER_EXEC={cod_ver_exec}, TIME_END={time_end}")
            except Exception as e:
                conn.rollback()
                print(f"Erro ao finalizar a execução na tabela {control_table}: {e}")



def run_script(script, data_exec, data_exec_ant, cod_cube, input_table, agreg_table, control_table):
    """
    Executa o script SQL fornecido, substituindo tabelas e parâmetros dinamicamente.

    :param script: Script SQL a ser executado
    :param data_exec: Data de execução
    :param cod_cube: Código do cubo (Ex: 0001, 0002, ...)
    :param input_table: Nome da tabela de entrada (Ex: EstabInc1)
    :param agreg_table: Nome da tabela de agregação (Ex: AgregNorm1)
    :param control_table: Nome da tabela de controle (Ex: ExecContrNorm1)
    :return: Dicionário com métricas de execução
    """
    with connect_to_db() as conn:
        try:
            with conn.cursor() as cursor:
                print(f"Iniciando execução do script para COD_CUBE={cod_cube}")

                # Substituir placeholders no script com nomes de tabelas
                script = script.replace("{input_table}", input_table) \
                    .replace("{agreg_table}", agreg_table) \
                    .replace("{control_table}", control_table) \
                    .replace("{data_exec_ant}", f"'{data_exec_ant}'") \
                    .replace("{data_exec}", f"'{data_exec}'")

                #cursor.execute(script)

                # Registrar horário de início da execução
                cube_start_time = datetime.now()
                print(f"Hora de início do cubo {cod_cube}: {cube_start_time}")

                # Reescrevendo o COUNT com a lógica de Incrementos e substituindo diretamente as datas
                count_query = f"""
                WITH 
                    Exclusoes AS (
                        SELECT * 
                        FROM {input_table}
                        WHERE DATA_FIM > '{data_exec_ant}' AND DATA_FIM <= '{data_exec}'
                    ),
                    Inclusoes AS (
                        SELECT * 
                        FROM {input_table}
                        WHERE DATA_INI > '{data_exec_ant}' AND DATA_INI <= '{data_exec}'
                    ),
                    Incrementos AS (
                        SELECT * FROM Exclusoes
                        UNION ALL
                        SELECT * FROM Inclusoes
                    )
                SELECT COUNT(*) FROM Incrementos;
                """

                # Executar o count diretamente com as datas já inseridas
                cursor.execute(count_query)
                registros_selecionados = cursor.fetchone()[0]
                print(f"Registros selecionados para o COD_CUBE={cod_cube}: {registros_selecionados}")

                # Medir tempo de execução completo (SELECT + INSERT)
                start_time = time.time()
                # Diagnóstico: exibir o script e os valores antes da execução
                #print("======= DIAGNÓSTICO SQL =======")
                #print(f"Script SQL:\n{script}")
                #print(f"Valores usados: data_exec_ant={data_exec_ant}, data_exec={data_exec}")
                #print("================================")
                cursor.execute(script, (data_exec_ant, data_exec))  # Usando ambas as datas com %s
                execution_time = time.time() - start_time

                # Registrar horário de término da execução
                cube_end_time = datetime.now()
                print(f"Hora de término do cubo {cod_cube}: {cube_end_time}")

                # Dividir tempos: SELECT e gravação
                # Supondo que o SELECT acontece antes de qualquer gravação no script
                select_time = execution_time * 0.6  # Estimar que 60% do tempo é gasto no SELECT
                write_time = execution_time * 0.4  # Estimar que 40% do tempo é gasto na gravação

                print(f"Script para COD_CUBE={cod_cube} executado com sucesso.")
                print(f"Tempo de SELECT estimado: {select_time:.2f} segundos.")
                print(f"Tempo de gravação estimado: {write_time:.2f} segundos.")
                print(f"Tempo total: {execution_time:.2f} segundos.")

                # Obter número de tuplas e valores gravados
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {agreg_table} WHERE COD_VER_EXEC = 
                    (SELECT MAX(COD_VER_EXEC) FROM {control_table}) AND COD_CUBE = %s;
                """, (cod_cube,))
                n_tuples = cursor.fetchone()[0]

                # Lógica fixa com base no script/cubo
                if cod_cube in ["0001", "0002", "0003"]:
                    n_values = n_tuples * 10  # Cubos 1, 2 e 3
                else:  # Cubos 4 e 5
                    n_values = n_tuples * 25

                print(f"  - COD_CUBE={cod_cube}: Tuplas gravadas={n_tuples}, Valores gravados={n_values}")

                # Retornar métricas detalhadas
                return {
                    "cod_cube": cod_cube,
                    "cube_start_time": cube_start_time,
                    "cube_end_time": cube_end_time,
                    "select_time": select_time,
                    "write_time": write_time,
                    "total_time": execution_time,
                    "n_tuples": n_tuples,
                    "n_values": n_values,
                    "registros_selecionados": registros_selecionados,  # Adicionado
                }
        except Exception as e:
            conn.rollback()
            print(f"Erro ao executar o script para COD_CUBE={cod_cube}: {e}")
            raise


def save_metrics_to_csv(metrics, cod_ver_exec, data_exec, cod_base, filename="etl_metrics_inctotal_inc1.csv"):
    """
    Salva as métricas coletadas durante a execução em um arquivo CSV.
    Adiciona uma linha de resumo geral para o COD_CUBE='0000'.

    :param metrics: Lista de métricas retornadas por run_script
    :param cod_ver_exec: Código da execução
    :param data_exec: Data da execução
    :param cod_base: Código base identificando o conjunto de tabelas
    :param filename: Nome do arquivo CSV onde as métricas serão salvas
    """
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)

        # Cabeçalho atualizado com nova ordem e campo extra
        if not file_exists:
            writer.writerow([
                "COD_BASE", "COD_VER_EXEC", "COD_CUBE", "DATA_EXEC",
                "CUBE_START_TIME", "CUBE_END_TIME",
                "SELECT_TIME", "WRITE_TIME", "TOTAL_TIME",
                "TUPLAS_GRAVADAS", "VALORES_GRAVADOS",
                "REGISTROS_SELECIONADOS"  # Novo campo
            ])

        # Adicionar métricas por cubo
        for metric in metrics:
            writer.writerow([
                cod_base,
                cod_ver_exec,
                metric["cod_cube"],
                data_exec.strftime("%Y-%m-%d %H:%M"),
                metric["cube_start_time"].strftime("%Y-%m-%d %H:%M:%S"),
                metric["cube_end_time"].strftime("%Y-%m-%d %H:%M:%S"),
                f"{metric['select_time']:.2f}",
                f"{metric['write_time']:.2f}",
                f"{metric['total_time']:.2f}",
                metric["n_tuples"],
                metric["n_values"],
                metric["registros_selecionados"]  # Novo valor adicionado
            ])

        # Adicionar linha de resumo geral (COD_CUBE='0000')
        total_tuples = sum(m["n_tuples"] for m in metrics)
        total_values = sum(m["n_values"] for m in metrics)
        total_select_time = sum(m["select_time"] for m in metrics)
        total_write_time = sum(m["write_time"] for m in metrics)
        total_time = sum(m["total_time"] for m in metrics)
        total_registros_selecionados = sum(m["registros_selecionados"] for m in metrics)  # Soma dos registros selecionados
        cube_start_time = min(m["cube_start_time"] for m in metrics)
        cube_end_time = max(m["cube_end_time"] for m in metrics)

        writer.writerow([
            cod_base,
            cod_ver_exec,
            "0000",  # Código do resumo geral
            data_exec.strftime("%Y-%m-%d %H:%M"),
            cube_start_time.strftime("%Y-%m-%d %H:%M:%S"),
            cube_end_time.strftime("%Y-%m-%d %H:%M:%S"),
            f"{total_select_time:.2f}",
            f"{total_write_time:.2f}",
            f"{total_time:.2f}",
            total_tuples,
            total_values,
            total_registros_selecionados  # Soma total dos registros selecionados
        ])

    print(f"Métricas salvas no arquivo: {filename}")


def main():
    """
    Função principal que executa o ETL em loop para cada conjunto de tabelas e datas.
    """
    for cod_base, (input_table, control_table, agreg_table) in enumerate(tables, start=1):
        print(f"Iniciando o processo para as tabelas: {input_table}, {control_table}, {agreg_table}")

        # Lista de datas para execução
        for data_exec_str in datas_execucao:
            try:
                data_exec = datetime.strptime(data_exec_str, "%Y-%m-%d %H:%M")

                # Capturar a data_exec_ant antes de iniciar uma nova execução
                with connect_to_db() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            SELECT MAX(DATA_EXEC) FROM {control_table}
                        """)
                        result = cursor.fetchone()[0]
                        if result is None:
                            print(f"Erro: Nenhuma versão anterior encontrada em {control_table}. Execução interrompida.")
                            continue
                        data_exec_ant = result

                print(f"Iniciando o processo para a data: {data_exec} com as tabelas: {input_table}, {control_table}, {agreg_table}")

                # Iniciar a execução com a nova data_exec
                cod_ver_exec, time_start, data_exec_ant = start_execution(data_exec, control_table)


                # Importar scripts SQL
                from SCRIPTB2 import script1, script2, script3

                # Substituir placeholders com as tabelas atuais e as datas de execução
                # Remover datas do .format() e passar via parâmetros seguros (%s)
                scripts_with_cod_cube = [
                    (script1.replace("{input_table}", input_table)
                     .replace("{control_table}", control_table)
                     .replace("{agreg_table}", agreg_table), "0001"),

                    (script2.replace("{input_table}", input_table)
                     .replace("{control_table}", control_table)
                     .replace("{agreg_table}", agreg_table), "0002"),

                    (script3.replace("{input_table}", input_table)
                     .replace("{control_table}", control_table)
                     .replace("{agreg_table}", agreg_table), "0003")
                ]

                # Executar os scripts
                metrics = []
                for script, cod_cube in scripts_with_cod_cube:
                    try:
                        # Validar SQL gerado
                        print(f"Executando COD_CUBE={cod_cube} para a data {data_exec.strftime('%Y-%m-%d %H:%M')}, utilizando as tabelas: "
                              f"Input: {input_table}, Control: {control_table}, Agreg: {agreg_table}.")

                        # Executar o script
                        metric = run_script(
                            script,
                            data_exec,
                            data_exec_ant,
                            cod_cube,
                            input_table,
                            agreg_table,
                            control_table
                        )
                        metrics.append(metric)
                    except Exception as e:
                        print(f"Erro ao executar o script para COD_CUBE={cod_cube}: {e}")

                # Finalizar a execução
                end_execution(cod_ver_exec, time_start, control_table, agreg_table)

                # Registrar métricas
                print("Métricas da execução:")
                for metric in metrics:
                    print(f"  - COD_CUBE={metric['cod_cube']}: "
                          f"Tempo Total={metric['total_time']:.2f}s, "
                          f"Tuplas={metric['n_tuples']}, "
                          f"Valores={metric['n_values']}")

                # Salvar métricas no CSV
                save_metrics_to_csv(metrics, cod_ver_exec, data_exec, cod_base)

            except ValueError as ve:
                print(f"Erro ao processar a data {data_exec_str}: {ve}")



if __name__ == "__main__":
    main()
