from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

def load_carga(ano):
    spark = SparkSession.builder.appName("LoadCarga").getOrCreate()
    bucket_name = "fiec-data-lake"
    input_path = f"s3a://{bucket_name}/silver/carga/{ano}/"
    
    jdbc_url = "jdbc:postgresql://postgres:5432/fiac_db"
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    
    df = spark.read.parquet(input_path)
    
    df.createOrReplaceTempView("carga_temp")
    
    upsert_query = """
    MERGE INTO carga_fato AS target
    USING (SELECT * FROM carga_temp) AS source
    ON target.id_carga = source.id_carga
    WHEN MATCHED THEN
        UPDATE SET 
            id_atracacao = source.id_atracacao,
            origem = source.origem,
            destino = source.destino,
            cd_mercadoria = source.cd_mercadoria,
            tipo_operacao = source.tipo_operacao,
            natureza_carga = source.natureza_carga,
            sentido = source.sentido,
            teu = source.teu,
            qt_carga = source.qt_carga,
            vl_peso_carga_bruta = source.vl_peso_carga_bruta
    WHEN NOT MATCHED THEN
        INSERT (id_carga, id_atracacao, origem, destino, cd_mercadoria, tipo_operacao, natureza_carga, sentido, teu, qt_carga, vl_peso_carga_bruta)
        VALUES (source.id_carga, source.id_atracacao, source.origem, source.destino, source.cd_mercadoria, source.tipo_operacao, source.natureza_carga, source.sentido, source.teu, source.qt_carga, source.vl_peso_carga_bruta);
    """
    
    spark.sql(upsert_query)
    print(f"Dados inseridos/atualizados na carga_fato para o ano {ano}")
    
if __name__ == "__main__":
    ano = sys.argv[1]
    load_carga(ano)
