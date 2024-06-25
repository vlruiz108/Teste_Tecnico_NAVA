from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, lag, coalesce
from pyspark.sql.window import Window


def create_spark_session():
    return SparkSession.builder.master("local").appName("SaldoClientes").getOrCreate()


def create_dataframe(spark, data, schema):
    return spark.createDataFrame(data, schema=schema)


def calculate_saldo(df):
    windowSpec = Window.partitionBy("cliente_id").orderBy("data")

    # Calcular saldo inicial usando a função `lag`
    df = df.withColumn("saldo_inicial", lag("saldo_final_atualizado", 1, 0).over(windowSpec))

    # Acumular as movimentações para calcular o saldo final
    df = df.withColumn("saldo_final", _sum("movimentacao").over(windowSpec))

    # Reprocessar saldos para considerar estornos
    df = df.withColumn("saldo_final", coalesce(df["saldo_final"], 0))
    df_reprocess = df.withColumn("saldo_final_atualizado", coalesce(df["saldo_inicial"], 0) + df["saldo_final"]).orderBy("data")

    return df_reprocess


def main():
    spark = create_spark_session()

    schema = ["data", "cliente_id", "movimentacao"]

    data = [
        ("2022-04-01", "Cliente 01", 100.00),
        ("2022-04-02", "Cliente 01", -50.00),
        ("2022-04-03", "Cliente 01", 50.00),
        ("2022-04-03", "Cliente 01", 50.00),
        ("2022-04-01", "Cliente 02", 200.00),
        ("2022-04-02", "Cliente 02", -100.00),
        ("2022-04-03", "Cliente 02", 100.00),
    ]

    df = create_dataframe(spark, data, schema)
    df_final = calculate_saldo(df)
    df_final.select("data", "cliente_id", "saldo_final_atualizado").orderBy("data", "cliente_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
