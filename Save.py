# %%
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import *

# %%
spark = SparkSession.builder.appName("transformando_dados").getOrCreate()

# %%
schema_clientes = StructType([
    StructField("CPF", StringType()),
    StructField("Nome", StringType()),
    StructField("Nascimento", StringType()),
    StructField("Sexo", StringType()),
    StructField("Est_civil", StringType()),
    StructField("Email", StringType()),
    StructField("Telefone", StringType()),
    StructField("CEP", StringType()),

])


cadastro_clientes = spark.read.csv("cadastro_clientes.csv", sep= ";", header=True, schema=schema_clientes)

# %%
cadastro_clientes.show()

# %%
cadastro_clientes.printSchema()

# %%
cadastro_clientes_transform = cadastro_clientes.select("*",f.regexp_replace("Telefone", "[^0-9]","")\
                                             .alias("Telefone_Consolidada"),f.regexp_replace("CPF","[.-]","")\
                                             .alias("CPF_Consolidada"),f.regexp_replace("CEP","[-]","").alias("CEP_Consolidada"),
                                            f.regexp_replace("Nascimento","[-]","").alias("Nascimento_Consolidada"))\
                                                .drop("Telefone","CPF", "CEP","Nascimento")

# %%
cadastro_clientes_transform.show()

# %%
cadastro_clientes_transform.printSchema()

# %%
schema_produtos = StructType([

    StructField("Numero", IntegerType()),
    StructField("Nome", StringType()),
    StructField("Genero", StringType()),
    StructField("Plataforma", StringType()),
    StructField("Data", IntegerType()),
    StructField("Preco", IntegerType()),
])




cadastro_produtos = spark.read.csv("cadastro_produto.csv", sep=";",header=True,schema=schema_produtos)

# %%
cadastro_produtos.show()

# %%
cadastro_produtos.printSchema()

# %%
nome_colunas = cadastro_produtos.columns
nome_colunas

# %%
condicional = [f.col(i).isNull() for i in nome_colunas]
condicional

# %%
condicional_final = f.functools.reduce(lambda x,y: x | y, condicional)
condicional_final

# %%
linhas_values_nulos = cadastro_produtos.filter(condicional_final)

# %%
linhas_values_nulos.show()

# %%
list = ['Soccer','Forgotten Echo','Fireshock']
cadastro_produtos.select("*").where(f.col("Nome").isin(list)).show(50)

# %%
cadastro_produtos.count()

# %%
cadastro_produtos.select("Nome","Plataforma","Data").where(((f.col("Nome") == "Soccer") & (f.col("Plataforma") == "PS")) | ((f.col("Nome") == "Forgotten Echo") & (f.col("Plataforma") == "PS")) | ((f.col("Nome") == "Fireshock") & (f.col("Plataforma") == "PS"))).show(40)

# %%

cadastro_produtos_updated = cadastro_produtos.select(
    "*",
    f.when((f.col("Nome") == "Soccer") & (f.col("Data").isNull()), f.lit(201709))\
    .when((f.col("Nome") == "Forgotten Echo") & (f.col("Data").isNull()), f.lit(201411))\
    .when((f.col("Nome") == "Fireshock") & (f.col("Data").isNull()), f.lit(201706))\
        .otherwise(f.col("Data"))\
            .alias("Data_Consolidada")\
).drop("Data")

cadastro_produtos_updated.show()


# %%
cadastro_produtos_updated.select("*").where(f.col("Data_Consolidada").isNull()).show()

# %%




