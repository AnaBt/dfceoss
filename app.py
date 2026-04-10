from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import duckdb

app = FastAPI()
templates = Jinja2Templates(directory="./project/templates")
con = duckdb.connect("project/db/cnpj.duckdb")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={"empresa": None})

@app.post("/", response_class=HTMLResponse)
async def buscar(
    request: Request,
    cnpj: str = Form(None),
    razao: str = Form(None),
    uf: str = Form(None),
    municipio: str = Form(None),
    situacao: str = Form(None),
    cnae: str = Form(None)  # <- CORREÇÃO 1: Adicionado aqui!
):
    # --- PASSO 1: LOCALIZAR A EMPRESA (COM DESCRIÇÃO DA NATUREZA) ---
    # Adicionamos o LEFT JOIN com a tabela 'naturezas'
    query_base = """
        SELECT 
            e.cnpj_basico, 
            e.razao_social, 
            e.natureza_juridica,
            n.descricao AS natureza_descricao, -- < TRÁS O NOME DA NATUREZA
            e.capital_social 
        FROM empresas_padronizado e
        LEFT JOIN naturezas n ON e.natureza_juridica = n.codigo
        WHERE 1=1
    """
    params_base = []

    if cnpj:
        cnpj_limpo = "".join(filter(str.isdigit, cnpj))[:8]
        query_base += " AND e.cnpj_basico = ?"
        params_base.append(cnpj_limpo)
    if razao:
        query_base += " AND e.razao_social ILIKE ?"
        params_base.append(f"{razao}%")

    query_base += " LIMIT 1"
    
    res_emp = con.execute(query_base, params_base).fetchone()
    
    if not res_emp:
        return templates.TemplateResponse("index.html", {"request": request, "erro": "Empresa não encontrada", "resultados": []})

    colunas_emp = [desc[0] for desc in con.description]
    empresa = dict(zip(colunas_emp, res_emp))
    id_cnpj = empresa['cnpj_basico']

    # --- PASSO 2: BUSCAR TODOS OS ESTABELECIMENTOS (COM DESCRIÇÃO CNAE E MUNICIPIO) ---
    # Adicionamos o LEFT JOIN com 'cnaes' e 'municipios'
    query_est = """
        SELECT DISTINCT 
            est.cnpj_ordem, 
            est.cnpj_dv, 
            est.nome_fantasia, 
            est.uf, 
            est.municipio,
            m.descricao AS municipio_nome, -- < TRÁS O NOME DA CIDADE
            est.situacao_cadastral, 
            est.cnae_principal,
            c.descricao AS cnae_descricao  -- < TRÁS A PROFISSÃO/ATIVIDADE
        FROM estabelecimentos_padronizado est
        LEFT JOIN municipios m ON est.municipio = m.codigo
        LEFT JOIN cnaes c ON est.cnae_principal = c.codigo
        WHERE est.cnpj_basico = ?
    """
    params_est = [id_cnpj]

    # ... (MANTENHA SEUS IFs DE FILTRO AQUI EXATAMENTE COMO ESTÃO) ...
    if uf:
        query_est += " AND est.uf = ?"
        params_est.append(uf.strip().upper())
    if municipio:
        query_est += " AND m.descricao ILIKE ?" # Filtra agora pelo NOME da cidade, muito melhor!
        params_est.append(f"%{municipio}%")
    if situacao:
        query_est += " AND est.situacao_cadastral = ?"
        params_est.append(situacao)
    if cnae:
        query_est += " AND est.cnae_principal = ?"
        params_est.append(cnae.strip())

    cursor_est = con.execute(query_est, params_est)
    colunas_est = [desc[0] for desc in cursor_est.description]
    estabelecimentos = [dict(zip(colunas_est, row)) for row in cursor_est.fetchall()]

    # --- PASSO 3: BUSCAR TODOS OS SÓCIOS ---
    # CORREÇÃO: Usando a tabela 'socios_padronizado' direto!
    query_soc = "SELECT DISTINCT nome_socio, qualificacao_socio FROM socios_padronizado WHERE cnpj_basico = ? AND nome_socio IS NOT NULL"
    cursor_soc = con.execute(query_soc, [id_cnpj])
    socios = [dict(zip([d[0] for d in cursor_soc.description], row)) for row in cursor_soc.fetchall()]

    # Retornamos um objeto único com tudo dentro
    return templates.TemplateResponse(
        request=request, 
        name="index.html", 
        context={
            "empresa": empresa,
            "estabelecimentos": estabelecimentos,
            "socios": socios
        }
    )