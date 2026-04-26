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
    #.appName("Transformacao_Bronze_Silver") \
    #.master("spark://spark-master:7077") \
    # 3. CREDENCIAIS
        #.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    if spark:
        print("Conexão criada!!!!")

    # Caminho do MinIO
    path_bronze = "s3a://bronze/18-04-2026/Extrato_conta_corrente_012026.csv"
    
    # Lendo arquivo no MinIO da camada Bronze
    df = spark.read.options(delimiter=";", header=True).csv(path_bronze)
    print(f"Lendo dados de: {path_bronze}")
    
    df.show()
    print(f"Total de linhas lidas: {df.count()}")

    # Lower case nos registros e renomeando a coluna com o lower case
    df = df.select('*', sf.lower("Lançamento")).withColumnRenamed("lower(Lançamento)", "lancamento")
    df = df.select('*', sf.lower("Tipo Lançamento")).withColumnRenamed("lower(Tipo Lançamento)", "tipoLancamento")
    
    # Dropando colunas que não estão sendo usadas
    df = df.drop('Lançamento', 'Tipo Lançamento', 'Detalhes', 'N° documento')
    
    #Retirando colunas nulas
    df = df.withColumn("tipoLancamento", 
    when(trim(col("tipoLancamento")) == "", lit(None))
    .otherwise(col("tipoLancamento"))
        )
    df = df.dropna(subset=["tipoLancamento"])

    # SUGESTÃO DA IA
    #   df = df.select(
    #   to_date(col("Data"), "dd/MM/yyyy").alias("data"),
    #   sf.lower(col("Lançamento")).alias("lancamento"),
    #   sf.lower(col("Tipo Lançamento")).alias("tipo_lancamento"),
    #   regexp_replace(col("Valor"), ",", ".").cast(DecimalType(10, 2)).alias("valor"))
    #   Mais performático definir a coluna ao invés de usar o select(*)

    df.show()

    # Convertendo colunas
    # Replace vírgula pra ponto
    df = df.withColumn("valor", regexp_replace(col("Valor"), ",", "."))

    # To Decimal
    df = df.withColumn("valor", col("valor").cast(DecimalType(10, 2)))

    # To Date
    df = df.withColumn("Data", to_date(col("Data"), "dd/MM/yyyy"))

    df.show()

    df.write.format("parquet").mode("overwrite").save("s3a://silver/extrato_limpo.parquet")

    print("--- PROCESSAMENTO FINALIZADO COM SUCESSO ---")
    
    spark.stop()




    # Separando dataframes de entrada e saída (ess parte vai pra camada gold - agregação e separação)

    # Entrada: 

    #df_entrada = df.filter(df.tipoLancamento == 'entrada')
    #print('Registros que entraram: ')
    #df_entrada.show()

    # Saída:

    #df_saida = df.filter(df.tipoLancamento == 'saída')
    #print('Registros que saíram:')
    #df_saida.show()

    #df.write.format("parquet").save("s3a://silver/extrato_limpo.parquet")

if __name__ == "__main__":
    main()