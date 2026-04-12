from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import duckdb

app = FastAPI()
templates = Jinja2Templates(directory="./project/templates")
con = duckdb.connect("db/cnpj.duckdb")

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
    cnae: str = Form(None),
    acao: str = Form("buscar_lista"),
    cnpj_alvo: str = Form(None)
):
    cnpj_para_dossie = None
    filtros_filiais = {"uf": uf, "municipio": municipio, "situacao": situacao, "cnae": cnae}

    # ==========================================
    # ETAPA 1: BUSCA DE LISTA (LIMIT 50)
    # ==========================================
    if acao == "buscar_lista":
        query_base = """
            SELECT 
                e.cnpj_basico, 
                e.razao_social, 
                e.natureza_juridica,
                n.descricao AS natureza_descricao
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

        query_base += " LIMIT 50"
        
        res_lista = con.execute(query_base, params_base).fetchall()
        
        if not res_lista:
            return templates.TemplateResponse(
                request=request, 
                name="index.html", 
                context={"erro": "Nenhuma empresa encontrada com os termos informados.", "resultados": []}
            )

        if len(res_lista) == 1:
            cnpj_para_dossie = res_lista[0][0]
        else:
            colunas_lista = [desc[0] for desc in con.description]
            empresas_encontradas = [dict(zip(colunas_lista, row)) for row in res_lista]
            # CORREÇÃO AQUI: Passando os parâmetros nomeados
            return templates.TemplateResponse(
                request=request,
                name="index.html", 
                context={"empresas_lista": empresas_encontradas, "filtros": filtros_filiais}
            )

    # ==========================================
    # ETAPA 2: CARREGAMENTO DO DOSSIÊ
    # ==========================================
    if acao == "ver_dossie" and cnpj_alvo:
        cnpj_para_dossie = cnpj_alvo

    if cnpj_para_dossie:
        # A. Matriz
        query_matriz = """
            SELECT e.cnpj_basico, e.razao_social, e.natureza_juridica, n.descricao AS natureza_descricao, e.capital_social 
            FROM empresas_padronizado e
            LEFT JOIN naturezas n ON e.natureza_juridica = n.codigo
            WHERE e.cnpj_basico = ? LIMIT 1
        """
        res_emp = con.execute(query_matriz, [cnpj_para_dossie]).fetchone()
        colunas_emp = [desc[0] for desc in con.description]
        empresa = dict(zip(colunas_emp, res_emp))

        # B. Estabelecimentos
        query_est = """
            SELECT DISTINCT est.cnpj_ordem, est.cnpj_dv, est.nome_fantasia, est.uf, est.municipio,
                   m.descricao AS municipio_nome, est.situacao_cadastral, est.cnae_principal, c.descricao AS cnae_descricao 
            FROM estabelecimentos_padronizado est
            LEFT JOIN municipios m ON est.municipio = m.codigo
            LEFT JOIN cnaes c ON est.cnae_principal = c.codigo
            WHERE est.cnpj_basico = ?
        """
        params_est = [cnpj_para_dossie]

        if uf:
            query_est += " AND est.uf = ?"
            params_est.append(uf.strip().upper())
        if municipio:
            query_est += " AND m.descricao ILIKE ?"
            params_est.append(f"%{municipio}%")
        if situacao:
            query_est += " AND est.situacao_cadastral = ?"
            params_est.append(situacao)
        if cnae:
            query_est += " AND est.cnae_principal = ?"
            params_est.append(cnae.strip())

        cursor_est = con.execute(query_est, params_est)
        estabelecimentos = [dict(zip([d[0] for d in cursor_est.description], row)) for row in cursor_est.fetchall()]

        # C. Sócios
        query_soc = "SELECT DISTINCT nome_socio, qualificacao_socio FROM socios_padronizado WHERE cnpj_basico = ? AND nome_socio IS NOT NULL"
        cursor_soc = con.execute(query_soc, [cnpj_para_dossie])
        socios = [dict(zip([d[0] for d in cursor_soc.description], row)) for row in cursor_soc.fetchall()]

        # CORREÇÃO AQUI: Passando os parâmetros nomeados
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={"empresa": empresa, "estabelecimentos": estabelecimentos, "socios": socios}
        )
