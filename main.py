from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, lit

# Inicialização do SparkSession
spark = SparkSession.builder \
    .appName("Calculo Saldo Conta Corrente") \
    .getOrCreate()

# Caminho dos arquivos
base_path = "C:/Users/Vanessa/Desktop/Teste_Tecnico_NAVA/bases/"
saldo_inicial_file = base_path + "tabela_saldo_inicial.txt"
movimentacoes_files = [
    base_path + "movimentacao_dia_02_04_2022.txt",
    base_path + "movimentacao_dia_03_04_2022.txt"
]

# Função para calcular saldo diário
def calcular_saldo_diario(saldo_inicial_file, movimentacoes_files):
    # Carregando o arquivo de saldo inicial
    saldo_inicial_df = spark.read.option("delimiter", ";").csv(saldo_inicial_file, header=True, inferSchema=True)
    
    # Calculando saldo inicial por CPF
    saldo_inicial = saldo_inicial_df.groupBy("CPF").agg(sum("Saldo_Inicial_CC").alias("Saldo_Inicial"))
    
    # Processando movimentações
    movimentacoes_df = None
    for mov_file in movimentacoes_files:
        mov_df = spark.read.option("delimiter", ";").csv(mov_file, header=True, inferSchema=True)
        if movimentacoes_df is None:
            movimentacoes_df = mov_df
        else:
            movimentacoes_df = movimentacoes_df.union(mov_df)
    
    # Juntando saldo inicial com movimentações
    saldo_atualizado = saldo_inicial.join(movimentacoes_df, on="CPF", how="left_outer") \
        .withColumn("Saldo_Final", when(col("Saldo_Inicial").isNull(), lit(0)).otherwise(col("Saldo_Inicial")) + col("Movimentacao_dia"))
    
    # Ordenando e mostrando resultados
    saldo_atualizado.orderBy("data", "CPF").show()

# Chamando a função principal
calcular_saldo_diario(saldo_inicial_file, movimentacoes_files)

# Encerrando a sessão Spark
spark.stop()

