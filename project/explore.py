import duckdb

con = duckdb.connect("db/cnpj.duckdb")

print(con.execute("SHOW TABLES").fetchall())