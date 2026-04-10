import os
import duckdb

BASE_DIR = os.path.dirname(__file__)  # pasta do ingest.py
DATA_DIR = os.path.join(BASE_DIR, "data")

empresas_path = os.path.join(DATA_DIR, "K3241.K03200Y1.D60314.EMPRECSV")
esta_path = os.path.join(DATA_DIR, "K3241.K03200Y1.D60314.ESTABELE")
socios_path = os.path.join(DATA_DIR, "K3241.K03200Y1.D60314.SOCIOCSV")
cnaes_path = os.path.join(DATA_DIR, "F.K03200$Z.D60314.CNAECSV")
municipios_path = os.path.join(DATA_DIR, "F.K03200$Z.D60314.MUNICCSV")
natureza_path = os.path.join(DATA_DIR, "F.K03200$Z.D60314.NATJUCSV")

# DICA DE SÊNIOR: Para evitar que o SQL confunda as barras invertidas do Windows (\) 
# com códigos de escape (como \n), vamos converter as barras para barras normais (/)
cnaes_path = cnaes_path.replace('\\', '/')
municipios_path = municipios_path.replace('\\', '/')
natureza_path = natureza_path.replace('\\', '/')

os.makedirs("db", exist_ok=True)
con = duckdb.connect("db/cnpj.duckdb")

print("Iniciando ingestão")
#empresas "00000053";"CONDOMINIO EDIFICIO MIRIAM";"3085";"19";"0,00";"05";""

con.execute(f"""
CREATE OR REPLACE TABLE empresas_padronizado AS 
SELECT
    column0 AS cnpj_basico,
    TRIM(UPPER(column1)) AS razao_social,
    column2 AS natureza_juridica,
    column3 AS qualificacao_responsavel,
    CAST(REPLACE(column4, ',', '.') AS DOUBLE) AS capital_social,
    column5 AS porte,
    NULLIF(TRIM(column6) , '') AS ente_federativo
FROM read_csv(
    "{empresas_path}",
    delim=';',
    header=False,
    encoding='latin-1',
    all_varchar=true
);
""")


print("✅ Empresas carregadas")

# ESTABELECIMENTOS
con.execute(f"""
CREATE OR REPLACE TABLE estabelecimentos_padronizado AS
SELECT
  column00 AS cnpj_basico,
  column01 AS cnpj_ordem,
  column02 AS cnpj_dv,
  UPPER(TRIM(column04)) AS nome_fantasia,
  UPPER(TRIM(column05)) AS situacao_cadastral,
  TRIM(column11) AS cnae_principal,
  UPPER(TRIM(column17)) AS bairro,
  UPPER(TRIM(column19)) AS uf,
  UPPER(TRIM(column20)) AS municipio
FROM read_csv(
  "{esta_path}",
  delim=';',
  header=False,
  encoding='latin-1',
  all_varchar=true  
);
""")

print("✅ Estabelecimentos carregados")

# SOCIOS
con.execute(f"""
CREATE OR REPLACE TABLE socios_padronizado AS
SELECT
  column00 AS cnpj_basico,
  UPPER(TRIM(column02)) AS nome_socio,
  UPPER(TRIM(column04)) AS qualificacao_socio
FROM read_csv(
  "{socios_path}",
  delim=';',
  header=False,
  encoding='latin-1',
  all_varchar=true
);
""")

print("✅ Sócios carregados")

# CNAES
con.execute(f"""
CREATE OR REPLACE TABLE cnaes AS SELECT
  column0 AS codigo,
  column1 AS descricao
FROM read_csv(
  '{cnaes_path}', -- < ASPAS SIMPLES AQUI!
  delim=';',
  header=False,
  encoding='latin-1',
  all_varchar=true
);
""")
print("✅ Cnaes carregados")


# MUNICIPIOS
con.execute(f"""
CREATE OR REPLACE TABLE municipios AS SELECT
  column0 AS codigo,
  column1 AS descricao
FROM read_csv(
  '{municipios_path}', -- < ASPAS SIMPLES AQUI!
  delim=';',
  header=False,
  encoding='latin-1',
  all_varchar=true
);
""")
print("✅ Municipios carregados")


# NATUREZA
con.execute(f"""
CREATE OR REPLACE TABLE naturezas AS SELECT
  column0 AS codigo,
  column1 AS descricao
FROM read_csv(
  '{natureza_path}', -- < ASPAS SIMPLES AQUI!
  delim=';',
  header=False,
  encoding='latin-1',
  all_varchar=true
);
""")
print("✅ Naturezas carregadas")

print("🎉 Ingestão finalizada!")

