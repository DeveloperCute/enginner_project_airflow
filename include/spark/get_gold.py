from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, regexp_replace, to_date, when, trim, lit
from pyspark.sql.types import DecimalType
import os

def main():
    # 1. Inicia a sessão Spark

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TesteLocal") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    if spark:
        print("Conexão criada!!!!")
    else:
        return 'Não foi possível estabelecer conexão com Spark.'

    # Caminho do MinIO
    path_silver = "s3a://silver/extrato_limpo.parquet"

    
    # Lendo arquivo no MinIO da camada Bronze
    df = spark.read.parquet(path_silver)

    print(df.show())
    #Agrupando valores de entrada
    agg_tipo_lancamento = df.groupBy("tipoLancamento").sum("valor")
    
    agg_tipo_lancamento.show()
    try:
        agg_tipo_lancamento.write.format("parquet").mode("overwrite").save("s3a://gold/extrato_tratato_agg.parquet")
        print('Arquivo salvo na camada gold!')
    except Exception as e:
        print('Erro ao salvar arquivo na camada gold!')
    spark.stop()

if __name__ == "__main__":
    main()