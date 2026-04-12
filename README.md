# 🏛️ Consulta CNPJ: Buscador e Dossiê Integrado

Este projeto é uma aplicação Full-Stack construída com **FastAPI**, **DuckDB** e **Jinja2** para lidar com os dados massivos da Receita Federal do Brasil (RFB).

## 🚧 O Problema com Dados Abertos (Aviso sobre os Scripts)

Se você olhar o código, vai notar que existem arquivos/scripts criados para **baixar** e **extrair** os dados direto do portal do governo. 

**Por que eles não fazem parte do fluxo principal?**
No mundo ideal, você rodaria um script e ele faria o download de tudo. Na prática, a Receita Federal implementa bloqueios rigorosos contra automação (firewalls, rate limits e bloqueios de IP) , coloquei os arquivos para mostrar que em uma coleta ideal os dados seriam obtidos/extraidos dessa forma.


⚠️Portanto, para testar esta aplicação, **os dados originais precisam ser baixados e extraidos manualmente** , os dados são:
* Empresa1
* Socios1 
* Estabelecimentos1
* Municipios
* Naturezas
* Cnaes

Os resultados extraidos devem ser colocados dentro da pasta  `data ` que esta dentro de `project `

## 🧠 Arquitetura e Estratégia de Memória

Lidar com o banco do CNPJ exige estratégia. Uma empresa matriz pode ter milhares de filiais e dezenas de sócios. 

**A solução implementada:**
1. **Consultas Granulares (Sob Demanda):** O backend faz buscas separadas (Matriz, depois Filiais, depois Sócios) usando `LEFT JOIN` com as tabelas de domínio apenas no momento da consulta.
2. **Design em 2 Estados (Buscador vs. Dossiê):** - Como a busca pode ser ampla , para o desafio o sistema limita a 50 resultados e traz apenas os dados básicos para a interface não travar.
   - Quando o usuário clica em "Ver Dossiê", a API vai  no CNPJ Básico selecionado e carrega as relações complexas (filiais e sócios) apenas daquela empresa.
   - O sistema está apto a trazer mais de 50 resultados , mas para não tomar muito tempo e mostrar a implementação funcionando , limitei o número de exibições.

## 📈 Bônus: Escalando o Banco de Dados (Adicionando Múltiplos Lotes)

Se você desejar expandir a base de dados além do "Lote 1" , siga os passos abaixo:

### 1. Download e Organização
1. Baixe os lotes adicionais (Empresas, Estabelecimentos e Sócios) no portal de Dados Abertos. *(Nota: Tabelas de Domínio como Municípios e CNAE são únicas e não precisam ser baixadas novamente).*
2. Extraia os arquivos na pasta `data/`.
3. adicione na `#SECAO DE MAPEAMENTO DE DADOS` os caminhos novos para empresa , sócios e estabelecimentos ex:
```bash
empresaslote2_path = os.path.join(DATA_DIR, "K3241.K03200Y1.D60314.EMPRECSV")
```
E por fim adicione  no `con.execute` o caminho correto ( comentário acima do comando especificando o correto ) , da seguinte forma : 
```bash
  FROM read_csv(
    "{empresas_path}",
    "{empresaslote2_path}",
    delim=';',
    header=False,
```

## ⚠️ Limitações do sistema
Para viabilizar a execução local e manter o repositório leve para avaliação, trabalhei **exclusivamente com os arquivos terminados em "1"** (Lote 1).
* A Receita Federal divide as empresas em lotes com base no final do CNPJ base. O Lote 1 representa cerca de **10%** das empresas brasileiras.
* **Atenção:** Se você buscar por uma grande empresa específica (ex: Itaú, Correios) e ela não retornar resultados, **é uma limitação no número de dados ingeridos**. Significa apenas que o CNPJ daquela empresa está em outro arquivo `.zip` (como o Lote 4 ou Lote 8).
* Para testar a interface de lista, experimente buscar por palavras comuns como: `PADARIA`, `ESCOLA`, `BANCO`.

## ⚙️ Como Executar o Projeto Localmente

### Pré-requisitos
* **Python 3.9+** instalado na máquina.
* Arquivos de dados do Lote 1 da Receita Federal já extraídos.

### Passo 1: Clonar o repositório e baixar bibliotecas
Abra o terminal e clone o projeto para a sua máquina:
```bash
git clone https://github.com/AnaBt/dfceoss
cd dfceoss

python -m venv .venv

# Para ativar no Windows:
.venv\Scripts\activate

# Para ativar no Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### Passo 2: Preparar os Dados Brutos (ETL)
Como os dados originais não estão versionados no GitHub devido ao tamanho:

* Coloque os arquivos descompactados da Receita Federal dentro desta pasta `data` (ex: K3241.K03200Y1.D60314.EMPRECSV, F.K03200$Z.D60314.CNAECSV, etc).

São eles:
* Empresa1
* Socios1 
* Estabelecimentos1
* Municipios
* Naturezas
* Cnaes

Descompacte todos , deverá se parecer com : 

data/  
├── F.K03200$Z.D60314.CNAECSV  
├── F.K03200$Z.D60314.MUNICCSV  
├── F.K03200$Z.D60314.NATJUCSV  
├── K3241.K03200Y1.D60314.EMPRECSV  
├── K3241.K03200Y1.D60314.ESTABELE  
└── K3241.K03200Y1.D60314.SOCIOCSV  

### Passo 3: Criar o Banco de Dados
Rode o script de ingestão. Ele vai ler os arquivos brutos, tratar as tipagens e popular o banco de dados analítico:
```bash
cd project
python ingest.py
```
(O script criará automaticamente uma pasta db contendo o arquivo cnpj.duckdb formatado e pronto para uso).

### Passo 4: Acessar o Sistema
```bash
cd ..
uvicorn app:app
```
Abra no seu navegador o link gerado pelo uvicorn 

