"""
Script de Benchmark para MySQL
Versão: 1.9
Autor: Acáciolr-DBA

Este script realiza operações de benchmark em um banco de dados MySQL,
incluindo criação de tabela, inserção de registros, criação de índices, etc.

Versões:
1.0 - [25-04-2024]: Versão inicial do script
1.1 - [25-04-2024]: Adicionado tratamento de exceções mais específico, refatoração do código,
                    comentários explicativos e sistema básico de versionamento.
1.2 - [25-04-2024]: Inclusão do drop table na função "create_table"
1.3 - [25-04-2024]: Ajustes na função de drop do index
1.4 - [25-04-2024]: Inclusão da rotina que simula lock na tabela
1.5 - [25-04-2024]: Inclusão da rotina de geração das metricas em txt - BETA
1.6 - [25-04-2024]: Atualização para criar arquivo de métricas antes de iniciar o benchmark
1.7 - [25-04-2024]: Correção na geração e atualização do arquivo de métricas
1.8 - [25-04-2024]: Inclusão das métricas do sistema no arquivo de métricas
1.9 - [25-04-2024]: Integração das métricas de sistema ao script de benchmark

"""

import time
import random
import mysql.connector
import psutil

# Configurações do banco de dados
db_config = {
    'user': 'monitor',
    'password': 'Welcome1',
    'host': '192.168.56.104',
    'database': 'mysql'  # Nome do banco de dados de benchmark
}

# Listas para armazenar as operações e métricas
operations_list = []
metrics_list = []

# Função para criar uma conexão com o banco de dados
def connect_to_db():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print("Erro ao conectar ao banco de dados:", err)
        return None

# Função para criar a tabela de benchmark
def create_table(cursor):
    try:
        cursor.execute("DROP TABLE IF EXISTS benchmark_table")
        cursor.execute("CREATE TABLE IF NOT EXISTS benchmark_table (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255))")
    except mysql.connector.Error as err:
        print("Erro ao criar tabela:", err)

# Função para inserir registros na tabela
def insert_records(cursor, num_records):
    try:
        for _ in range(num_records):
            data = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=10))
            cursor.execute("INSERT INTO benchmark_table (data) VALUES (%s)", (data,))
        return True
    except mysql.connector.Error as err:
        print("Erro ao inserir registros:", err)
        return False

# Função para criar um índice na tabela
def create_index(cursor):
    try:
        cursor.execute("CREATE INDEX idx_data ON benchmark_table (data)")
        return True
    except mysql.connector.Error as err:
        print("Erro ao criar índice:", err)
        return False

# Função para excluir um índice na tabela
def drop_index(cursor):
    try:
        cursor.execute("SHOW INDEX FROM benchmark_table WHERE Key_name = 'idx_data'")
        result = cursor.fetchone()
        if result:
            cursor.execute("DROP INDEX idx_data ON benchmark_table")
            return True
        else:
            print("Índice 'idx_data' não existe.")
            return False
    except mysql.connector.Error as err:
        print("Erro ao excluir índice:", err)
        return False

# Função para calcular métricas de benchmark
def calculate_metrics(start_time, end_time, total_operations, successful_operations):
    total_time = end_time - start_time
    ops_per_sec = total_operations / total_time
    avg_exec_time = total_time / successful_operations if successful_operations > 0 else 0
    return total_time, ops_per_sec, avg_exec_time

# Função para obter métricas de sistema
def get_system_metrics():
    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_count = psutil.cpu_count()

    mem = psutil.virtual_memory()
    total_mem = mem.total / (1024 * 1024)
    available_mem = mem.available / (1024 * 1024)
    used_mem = mem.used / (1024 * 1024)

    return cpu_percent, cpu_count, total_mem, available_mem, used_mem

# Função para executar operações de benchmark
def run_benchmark():
    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                create_table(cursor)
                if not insert_records(cursor, 100000):
                    return
                
                total_operations = 0
                successful_operations = 0
                start_time = time.time()
                end_time = start_time + 300  # Executa por 5 minutos
                
                cpu_percent_list = []
                memory_percent_list = []
                
                print("Métricas de Benchmark:")
                
                while time.time() < end_time:
                    operation = random.choice(['update', 'insert', 'delete', 'select', 'create_index', 'drop_index', 'lock'])
                    total_operations += 1
                    print("Operação:", operation)
                    operations_list.append(operation)
                    
                    # Obter métricas do sistema
                    cpu_percent, cpu_count, total_mem, available_mem, used_mem = get_system_metrics()
                    cpu_percent_list.append(cpu_percent)
                    memory_percent_list.append((total_mem, available_mem, used_mem))
                    
                    try:
                        if operation == 'update':
                            cursor.execute("UPDATE benchmark_table SET data = %s WHERE id = %s", (random.choice('abcdefghijklmnopqrstuvwxyz'), random.randint(1, 100000)))
                        elif operation == 'insert':
                            data = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=10))
                            cursor.execute("INSERT INTO benchmark_table (data) VALUES (%s)", (data,))
                        elif operation == 'delete':
                            cursor.execute("DELETE FROM benchmark_table WHERE id = %s", (random.randint(1, 100000),))
                        elif operation == 'select':
                            cursor.execute("SELECT * FROM benchmark_table WHERE id = %s", (random.randint(1, 100000),))
                            cursor.fetchall()  # Consumir resultados
                        elif operation == 'create_index':
                            create_index(cursor)
                        elif operation == 'drop_index':
                            drop_index(cursor)
                        else:  # lock
                            print("Bloqueio de tabela por 2 minutos")
                            cursor.execute("LOCK TABLES benchmark_table WRITE")
                            time.sleep(120)  # Lock por 2 minutos
                            cursor.execute("UNLOCK TABLES")
                        successful_operations += 1
                    except mysql.connector.Error as err:
                        print("Erro durante a operação:", err)
                    
                    # Aguarda um pequeno intervalo antes de continuar
                    time.sleep(0.1)
                
                total_time, ops_per_sec, avg_exec_time = calculate_metrics(start_time, end_time, total_operations, successful_operations)
                metrics_list.append("\nMétricas finais:")
                metrics_list.append("Tempo total de execução: {} segundos".format(total_time))
                metrics_list.append("Número total de operações: {}".format(total_operations))
                metrics_list.append("Número de operações bem-sucedidas: {}".format(successful_operations))
                metrics_list.append("Taxa de operações por segundo: {}".format(ops_per_sec))
                metrics_list.append("Tempo médio de execução por operação: {} segundos".format(avg_exec_time))
                
                # Métricas de sistema
                metrics_list.append("\nMétricas de Sistema:")
                metrics_list.append("Consumo médio de CPU: {}%".format(sum(cpu_percent_list) / len(cpu_percent_list)))
                metrics_list.append("Número de CPUs: {}".format(cpu_count))
                for idx, (total_mem, available_mem, used_mem) in enumerate(memory_percent_list):
                    metrics_list.append("Execução {}: ".format(idx + 1))
                    metrics_list.append("  Total de memória: {} MB".format(total_mem))
                    metrics_list.append("  Memória disponível: {} MB".format(available_mem))
                    metrics_list.append("  Memória usada: {} MB".format(used_mem))
                
                print("\n".join(metrics_list))
                print("\nOperações realizadas:", operations_list)
                print("\nBenchmark concluído.")
        finally:
            connection.close()

if __name__ == "__main__":
    run_benchmark()
