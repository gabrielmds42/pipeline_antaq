from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
from dotenv import load_dotenv


load_dontenv()

EMAIL_ENVIO = os.getenv('EMAIL_ENVIO')
ENDPOINT_URL = os.getenv('ENDPOINT_URL')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def load_atracacao(ano):
    spark = SparkSession.builder.appName("load_atracacao").getOrCreate()
    bucket_name = f"{BUCKET_NAME}"
    input_path = f"s3a://{bucket_name}/silver/atracacao/{ano}/"



    jdbc_url = "jdbc:postgresql://postgres:5432/fiac_db"
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    
    df = spark.read.parquet(input_path)
    
    df.createOrReplaceTempView("atracacao_temp")
    
    upsert_query = """
    MERGE INTO atracacao_fato AS target
    USING (SELECT * FROM atracacao_temp) AS source
    ON target.id_atracacao = source.id_atracacao
    WHEN MATCHED THEN
        UPDATE SET 
            cd_tup = source.cd_tup,
            id_berco = source.id_berco,
            berco = source.berco,
            porto_atracacao = source.porto_atracacao,
            apelido_instalacao = source.apelido_instalacao,
            complexo_portuario = source.complexo_portuario,
            tipo_autoridade_portuaria = source.tipo_autoridade_portuaria,
            data_atracacao = source.data_atracacao,
            data_chegada = source.data_chegada,
            data_desatracacao = source.data_desatracacao,
            ano = source.ano,
            mes = source.mes,
            tipo_operacao = source.tipo_operacao,
            tipo_navegacao = source.tipo_navegacao,
            nacionalidade_armador = source.nacionalidade_armador,
            municipio = source.municipio,
            uf = source.uf,
            sigla_uf = source.sigla_uf,
            regiao_geografica = source.regiao_geografica
    WHEN NOT MATCHED THEN
        INSERT (id_atracacao, cd_tup, id_berco, berco, porto_atracacao, apelido_instalacao, complexo_portuario, tipo_autoridade_portuaria, data_atracacao, data_chegada, data_desatracacao, ano, mes, tipo_operacao, tipo_navegacao, nacionalidade_armador, municipio, uf, sigla_uf, regiao_geografica)
        VALUES (source.id_atracacao, source.cd_tup, source.id_berco, source.berco, source.porto_atracacao, source.apelido_instalacao, source.complexo_portuario, source.tipo_autoridade_portuaria, source.data_atracacao, source.data_chegada, source.data_desatracacao, source.ano, source.mes, source.tipo_operacao, source.tipo_navegacao, source.nacionalidade_armador, source.municipio, source.uf, source.sigla_uf, source.regiao_geografica);
    """
    
    spark.sql(upsert_query)
    print(f"Dados inseridos/atualizados na atracacao_fato para o ano {ano}")
    
if __name__ == "__main__":
    ano = sys.argv[1]
    load_atracacao(ano)
