from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import sys

def process_atracacao(ano):
    spark = SparkSession.builder.appName("ProcessAtracacao").getOrCreate()
    bucket_name = "fiec-data-lake"
    input_path = f"s3a://{bucket_name}/bronze/atracacao/{ano}/file.txt"
    output_path = f"s3a://{bucket_name}/silver/atracacao/{ano}/"

    df = spark.read.option("header", "true").option("delimiter", ";").csv(input_path)

    df = df.select(
        col("IDAtracacao").alias("id_atracacao").cast("int"),
        col("CDTUP").alias("cd_tup"),
        col("IDBerco").alias("id_berco"),
        col("Berço").alias("berco"),
        col("Porto Atracação").alias("porto_atracacao"),
        col("Apelido Instalação Portuária").alias("apelido_instalacao"),
        col("Complexo Portuário").alias("complexo_portuario"),
        col("Tipo da Autoridade Portuária").alias("tipo_autoridade_portuaria"),
        to_timestamp(col("Data Atracação"), "dd/MM/yyyy HH:mm:ss").alias("data_atracacao"),
        to_timestamp(col("Data Chegada"), "dd/MM/yyyy HH:mm:ss").alias("data_chegada"),
        to_timestamp(col("Data Desatracação"), "dd/MM/yyyy HH:mm:ss").alias("data_desatracacao"),
        col("Ano").alias("ano").cast("int"),
        col("Mes").alias("mes"),
        col("Tipo de Operação").alias("tipo_operacao"),
        col("Tipo de Navegação da Atracação").alias("tipo_navegacao"),
        col("Nacionalidade do Armador").alias("nacionalidade_armador"),
        col("Município").alias("municipio"),
        col("UF").alias("uf"),
        col("SGUF").alias("sigla_uf"),
        col("Região Geográfica").alias("regiao_geografica"),
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Dados processados e salvos em: {output_path}")

if __name__ == "__main__":
    ano = sys.argv[1]
    process_atracacao(ano)
