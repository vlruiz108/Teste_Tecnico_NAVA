from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, lag
from pyspark.sql.window import Window

# Criar sessão do Spark
spark = SparkSession.builder.master("local").appName("SaldoClientes").getOrCreate()

# Definir esquema dos dados
schema = ["data", "cliente_id", "movimentacao"]

# Simular dados de movimentações diárias
data = [
    ("2022-04-01", "Cliente 01", 100.00),
    ("2022-04-02", "Cliente 01", -50.00),
    ("2022-04-03", "Cliente 01", 50.00),
    ("2022-04-03", "Cliente 01", 50.00),
    ("2022-04-01", "Cliente 02", 200.00),
    ("2022-04-02", "Cliente 02", -100.00),
    ("2022-04-03", "Cliente 02", 100.00),
]

# Criar DataFrame
df = spark.createDataFrame(data, schema=schema)

# Ordenar por data e cliente
windowSpec = Window.partitionBy("cliente_id").orderBy("data")

# Calcular saldo inicial usando a função `lag`
df = df.withColumn("saldo_inicial", lag("movimentacao").over(windowSpec))
df = df.withColumn("saldo_inicial", col("saldo_inicial").fillna(0))

# Acumular as movimentações para calcular o saldo final
df = df.withColumn("saldo_final", _sum("movimentacao").over(windowSpec))

# Reprocessar saldos para considerar estornos
df_reprocess = df.groupBy("cliente_id", "data").agg(
    _sum("movimentacao").alias("movimentacao_diaria"),
    _sum("saldo_final").alias("saldo_final")
).orderBy("data")

# Juntar saldos diários com a movimentação para o saldo final atualizado
df_final = df_reprocess.withColumn("saldo_final_atualizado", _sum("movimentacao_diaria").over(windowSpec))

# Mostrar resultado final
df_final.select("data", "cliente_id", "saldo_final_atualizado").show()

# Parar a sessão do Spark
spark.stop()
