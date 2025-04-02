from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

def process_carga(ano):
    spark = SparkSession.builder.appName("ProcessCarga").getOrCreate()
    bucket_name = "fiec-data-lake"
    input_path = f"s3a://{bucket_name}/bronze/carga/{ano}/file.txt"
    output_path = f"s3a://{bucket_name}/silver/carga/{ano}/"

    df = spark.read.option("header", "true").option("delimiter", ";").csv(input_path)

    df = df.select(
        col("IDCarga").alias("id_carga").cast("int"),
        col("IDAtracacao").alias("id_atracacao").cast("int"),
        col("Origem").alias("origem"),
        col("Destino").alias("destino"),
        col("CDMercadoria").alias("cd_mercadoria"),
        col("Tipo Operação da Carga").alias("tipo_operacao"),
        col("Natureza da Carga").alias("natureza_carga"),
        col("Sentido").alias("sentido"),
        col("TEU").alias("teu").cast("int"),
        col("QTCarga").alias("qt_carga").cast("int"),
        col("VLPesoCargaBruta").alias("vl_peso_carga_bruta").cast("double"),
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Dados processados e salvos em: {output_path}")

if __name__ == "__main__":
    ano = sys.argv[1]
    process_carga(ano)
