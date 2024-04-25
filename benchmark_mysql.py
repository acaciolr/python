import time
import random
import mysql.connector
import psutil
import os

# Configurações do banco de dados
db_config = {
    'user': 'monitor',
    'password': 'Welcome1',
    'host': '192.168.56.104',
    'database': 'mysql'  # Nome do banco de dados de benchmark
}

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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_data ON benchmark_table (data)")
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
    memory_percent = psutil.virtual_memory().percent
    return cpu_percent, memory_percent

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
                
                while time.time() < end_time:
                    operation = random.choice(['update', 'insert', 'delete', 'select', 'create_index', 'drop_index', 'lock'])
                    total_operations += 1
                    print("Operação:", operation)
                    
                    # Obter métricas do sistema
                    cpu_percent, memory_percent = get_system_metrics()
                    cpu_percent_list.append(cpu_percent)
                    memory_percent_list.append(memory_percent)
                    
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
                    
                    connection.commit()  # Garantir que as operações sejam aplicadas imediatamente
                        
                    # Aguarda um pequeno intervalo antes de continuar
                    time.sleep(0.1)
                
                total_time, ops_per_sec, avg_exec_time = calculate_metrics(start_time, end_time, total_operations, successful_operations)
                print("\nMétricas de Benchmark:")
                print("Tempo total de execução:", total_time, "segundos")
                print("Número total de operações:", total_operations)
                print("Número de operações bem-sucedidas:", successful_operations)
                print("Taxa de operações por segundo:", ops_per_sec)
                print("Tempo médio de execução por operação:", avg_exec_time, "segundos")
                
                # Métricas de sistema
                print("\nMétricas de Sistema:")
                print("Consumo médio de CPU:", sum(cpu_percent_list) / len(cpu_percent_list), "%")
                print("Consumo médio de memória:", sum(memory_percent_list) / len(memory_percent_list), "%")
                
                # Escrever métricas em um arquivo de texto
                with open("metrics.txt", "w") as f:
                    f.write("Métricas de Benchmark:\n")
                    f.write("Tempo total de execução: {} segundos\n".format(total_time))
                    f.write("Número total de operações: {}\n".format(total_operations))
                    f.write("Número de operações bem-sucedidas: {}\n".format(successful_operations))
                    f.write("Taxa de operações por segundo: {}\n".format(ops_per_sec))
                    f.write("Tempo médio de execução por operação: {} segundos\n".format(avg_exec_time))
                    f.write("\nMétricas de Sistema:\n")
                    f.write("Consumo médio de CPU: {}%\n".format(sum(cpu_percent_list) / len(cpu_percent_list)))
                    f.write("Consumo médio de memória: {}%\n".format(sum(memory_percent_list) / len(memory_percent_list)))
                
                # Abrir o arquivo de métricas após gerá-lo
                os.system("open metrics.txt")
        finally:
            connection.close()

if __name__ == "__main__":
    run_benchmark()
