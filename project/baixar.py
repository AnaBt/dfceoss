import requests
import os
import time
import zipfile

BASE_URL = "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9/download?path=%2F&files="

PASTA = "data"
os.makedirs(PASTA, exist_ok=True)

CHUNK_SIZE = 1024 * 1024  # 1MB

# sessão persistente (melhor estabilidade)
session = requests.Session()

# headers simulando navegador (CRÍTICO)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/octet-stream",
    "Connection": "keep-alive"
})


def arquivo_valido(caminho):
    """Verifica se o ZIP não está corrompido"""
    try:
        with zipfile.ZipFile(caminho, 'r') as z:
            z.testzip()
        return True
    except:
        return False


def baixar(nome_arquivo, tentativas=5):
    url = BASE_URL + nome_arquivo
    caminho = os.path.join(PASTA, nome_arquivo)

    # se já existe e está OK, pula
    if os.path.exists(caminho) and arquivo_valido(caminho):
        print(f"⏭️ Já existe e está OK: {nome_arquivo}")
        return

    for tentativa in range(tentativas):
        try:
            print(f"\n📥 Baixando {nome_arquivo} (tentativa {tentativa+1})")

            with session.get(url, stream=True, timeout=(10, 300), allow_redirects=True) as r:
                r.raise_for_status()

                content_type = r.headers.get("Content-Type", "")
                if "zip" not in content_type and "octet-stream" not in content_type:
                    raise Exception("Resposta não é um ZIP (veio HTML ou erro)")

                total = int(r.headers.get("Content-Length", 0))
                baixado = 0

                with open(caminho, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            baixado += len(chunk)

                            if total > 0:
                                progresso = (baixado / total) * 100
                                print(f"\r📊 {progresso:.2f}% ({baixado//(1024*1024)} MB)", end="")

            print(f"\n🔍 Validando ZIP...")
            if not arquivo_valido(caminho):
                raise Exception("ZIP corrompido após download")

            print(f"✅ Concluído: {nome_arquivo}")
            return

        except Exception as e:
            print(f"\n⚠️ Erro: {e}")

            if tentativa < tentativas - 1:
                print("🔁 Tentando novamente em 5s...")
                time.sleep(5)
            else:
                print("❌ Falhou definitivamente")


# 📦 escolha dos arquivos (comece leve!)
arquivos = [
    "Cnaes.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Empresas0.zip"
]


if __name__ == "__main__":
    print("🚀 Iniciando downloads...\n")

    for arq in arquivos:
        baixar(arq)
        time.sleep(2)

    print("\n🎉 Finalizado!")