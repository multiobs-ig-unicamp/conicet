#!/usr/bin/env python3
"""
authors_data_scraper.py
Coleta links E dados dos autores do Conicet em um único processo.

"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import json
import re
import argparse
from datetime import datetime, date, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Optional Parquet
try:
    import pyarrow.parquet as pq
    import pandas as pd
    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False

# ----------------- Config -----------------
OUTPUT_DIR = "saida_conicet_autores"
LOG_DIR = os.path.join(OUTPUT_DIR, "logs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

CSV_FILE = os.path.join(OUTPUT_DIR, "autores_completo.csv")
PARQUET_FILE = os.path.join(OUTPUT_DIR, "autores_completo.parquet")
ERROR_FILE = os.path.join(OUTPUT_DIR, "erros.csv")
STATE_FILE = os.path.join(OUTPUT_DIR, "estado.json")
PREVISAO_FILE = os.path.join(OUTPUT_DIR, "previsao.txt")

BASE_URL = "https://ri.conicet.gov.ar/explorar-autores?field=null&offset="
PAGE_SIZE = 90
WAIT_SECONDS = 1
WAIT_SELENIUM = 2
TZ_OFFSET = -3

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

CSV_COLUMNS = [
    # Identificação
    "Autor",
    "Referencia",
    "Link Principal",
    
    # Status
    "Conicet",
    
    # Informações Profissionais
    "Titulo",
    "Grado",
    "Especialidade",
    "Campo de Aplicacao",
    "Local de Trabalho",
    
    # Publicações
    "Quantidade de Handles",
    "Handles"
]

# ----------------- Logging -----------------
def log_line(message):
    today = date.today().isoformat()
    logfile = os.path.join(LOG_DIR, f"{today}.log")
    ts = datetime.utcnow().isoformat()
    line = f"{ts} - {message}"
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

def write_previsao(offset, total, start_time, autores_processados):
    """Escreve previsão de término atualizada"""
    if autores_processados <= 0:
        return
    
    elapsed = time.time() - start_time
    avg_per_autor = elapsed / autores_processados
    
    pagina_atual = (offset // PAGE_SIZE)
    total_pages = (total // PAGE_SIZE) + (1 if total % PAGE_SIZE else 0)
    percent = (pagina_atual / total_pages) * 100 if total_pages > 0 else 0
    
    autores_restantes = total - autores_processados
    est_seconds = autores_restantes * avg_per_autor
    
    termino_utc = datetime.utcnow() + timedelta(seconds=est_seconds)
    termino_local = termino_utc + timedelta(hours=TZ_OFFSET)
    
    elapsed_horas = elapsed / 3600
    est_horas = est_seconds / 3600
    
    with open(PREVISAO_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("PREVISÃO DE CONCLUSÃO - CONICET SCRAPER\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("PROGRESSO:\n")
        f.write(f"  • Autores processados: {autores_processados:,} / {total:,}\n")
        f.write(f"  • Percentual: {percent:.2f}%\n")
        f.write(f"  • Páginas: {pagina_atual:,} / {total_pages:,}\n\n")
        
        f.write("TEMPO:\n")
        f.write(f"  • Tempo decorrido: {elapsed_horas:.2f}h\n")
        f.write(f"  • Tempo restante: {est_horas:.2f}h\n")
        f.write(f"  • Tempo total estimado: {(elapsed_horas + est_horas):.2f}h\n")
        f.write(f"  • Velocidade média: {avg_per_autor:.2f}s por autor\n\n")
        
        f.write("CONCLUSÃO PREVISTA:\n")
        f.write(f"  • Data/Hora (UTC-3): {termino_local.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"  • Data/Hora (UTC): {termino_utc.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"Última atualização: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
        f.write("=" * 60 + "\n")

# ----------------- Estado -----------------
def carregar_estado():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log_line(f"AVISO: falha ao ler estado: {e}")
    return {"ultimo_offset": 0, "processados": {}, "total_autores": 0}

def salvar_estado(estado):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(estado, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)

# ----------------- CSV e Parquet helpers -----------------
def initialize_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

def append_csv_row(row):
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)

def append_parquet_row(row):
    """Atualiza Parquet incrementalmente"""
    if not HAS_PYARROW:
        return
    
    try:
        # Cria DataFrame com a nova linha
        df_new = pd.DataFrame([row], columns=CSV_COLUMNS)
        
        # Se já existe, carrega e concatena
        if os.path.exists(PARQUET_FILE):
            df_existing = pd.read_parquet(PARQUET_FILE)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
        
        # Salva
        df_combined.to_parquet(PARQUET_FILE, index=False, engine="pyarrow", compression="snappy")
    except Exception as e:
        log_line(f"AVISO: falha ao atualizar Parquet: {e}")

def log_error(url, erro):
    erro_data = {"url": url, "erro": str(erro), "timestamp": datetime.utcnow().isoformat()}
    if not os.path.exists(ERROR_FILE):
        with open(ERROR_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["url", "erro", "timestamp"])
            writer.writeheader()
    with open(ERROR_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "erro", "timestamp"])
        writer.writerow(erro_data)

# ----------------- Selenium -----------------
def configurar_driver():
    """Tenta configurar driver automaticamente (Edge, Chrome ou Firefox)"""
    
    # Tenta Edge local
    edge_path = "./msedgedriver"
    if os.path.exists(edge_path):
        try:
            log_line("Tentando Edge local...")
            options = webdriver.EdgeOptions()
            options.use_chromium = True
            options.add_argument("--log-level=3")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            service = Service(edge_path)
            return webdriver.Edge(service=service, options=options)
        except Exception as e:
            log_line(f"Edge local falhou: {e}")
    
    # Tenta Edge do sistema
    try:
        log_line("Tentando Edge do sistema...")
        options = webdriver.EdgeOptions()
        options.use_chromium = True
        options.add_argument("--log-level=3")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        return webdriver.Edge(options=options)
    except Exception as e:
        log_line(f"Edge do sistema falhou: {e}")
    
    # Tenta Chrome
    try:
        log_line("Tentando Chrome...")
        options = webdriver.ChromeOptions()
        options.add_argument("--log-level=3")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        return webdriver.Chrome(options=options)
    except Exception as e:
        log_line(f"Chrome falhou: {e}")
    
    # Tenta Firefox
    try:
        log_line("Tentando Firefox...")
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        return webdriver.Firefox(options=options)
    except Exception as e:
        log_line(f"Firefox falhou: {e}")
    
    raise RuntimeError(
        "Nenhum driver disponível!\n"
        "Instale um dos seguintes:\n"
        "  - Edge: apt install microsoft-edge-stable (+ webdriver-manager)\n"
        "  - Chrome: apt install chromium-browser chromium-chromedriver\n"
        "  - Firefox: apt install firefox firefox-geckodriver\n"
        "Ou baixe msedgedriver e coloque na pasta do projeto"
    )

# ----------------- Detectar total -----------------
def obter_total_autores(max_retries=5):
    """Tenta obter total de autores com retry"""
    for tentativa in range(max_retries):
        try:
            log_line(f"Tentando obter total de autores (tentativa {tentativa + 1}/{max_retries})...")
            r = requests.get(BASE_URL + "0", headers=HEADERS, timeout=20)
            r.raise_for_status()
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            
            patterns = [
                r"[Dd]el\s+\d+\s+[Aa]l\s+\d+\s+[Dd]e\s+([\d\.,]+)",
                r"Mostrando\s+ítems.*?de\s+([\d\.,]+)"
            ]
            for p in patterns:
                m = re.search(p, text)
                if m:
                    num = m.group(1).replace(".", "").replace(",", "")
                    if num.isdigit():
                        return int(num)
            
            nums = re.findall(r"\d{3,}", text)
            if nums:
                return max(int(n.replace(".", "")) for n in nums)
        
        except requests.exceptions.HTTPError as e:
            if tentativa < max_retries - 1:
                wait_time = (2 ** tentativa) * 2  # 2s, 4s, 8s, 16s, 32s
                log_line(f"ERRO HTTP {e.response.status_code}: aguardando {wait_time}s antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                log_line(f"ERRO: falha após {max_retries} tentativas: {e}")
        except Exception as e:
            log_line(f"ERRO: falha ao obter total: {e}")
            if tentativa < max_retries - 1:
                time.sleep(2 ** tentativa)
    
    return None

# ----------------- Coleta de uma página -----------------
def obter_links_pagina(offset, max_retries=3):
    """Obtém links de autores de uma página com retry"""
    url = f"{BASE_URL}{offset}"
    
    for tentativa in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=re.compile(r"(author\/|filtertype=author)", re.I))
            
            autores = []
            for a in links:
                nome = (a.text or "").strip()
                href = a.get("href") or ""
                if nome and href:
                    if href.startswith("/"):
                        href = "https://ri.conicet.gov.ar" + href
                    autores.append({"nome": nome, "link": href})
            
            return autores
        
        except requests.exceptions.HTTPError as e:
            if tentativa < max_retries - 1:
                wait_time = (2 ** tentativa) * 2
                log_line(f"ERRO_HTTP (tentativa {tentativa + 1}): offset={offset} erro={e.response.status_code}, aguardando {wait_time}s...")
                time.sleep(wait_time)
            else:
                log_line(f"ERRO_HTTP: offset={offset} erro={e}")
                log_error(url, e)
        except Exception as e:
            if tentativa < max_retries - 1:
                wait_time = 2 ** tentativa
                log_line(f"ERRO (tentativa {tentativa + 1}): offset={offset}, aguardando {wait_time}s...")
                time.sleep(wait_time)
            else:
                log_line(f"ERRO: offset={offset} erro={e}")
                log_error(url, e)
    
    return []

# ----------------- Coleta de dados do autor -----------------
def coletar_dados_autor(driver, nome, link):
    """Coleta dados detalhados de um autor usando Selenium"""
    try:
        driver.get(link)
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        page_source = driver.page_source
        if any(err in page_source for err in ["Proxy Error", "502 Bad Gateway", "invalid response"]):
            log_line(f"Erro de proxy em {link}")
            return None
        
        # Extrai referência e verifica credencial
        if "author/" in link:
            referencia = link.split("author/")[-1]
            try:
                tem_credencial = len(driver.find_elements(By.TAG_NAME, "img")) > 0
            except:
                tem_credencial = False
        else:
            referencia = ""
            tem_credencial = False
        
        autor = {
            "Autor": nome,
            "Referencia": referencia,
            "Link Principal": link,
            "Conicet": tem_credencial,
            "Titulo": "",
            "Grado": "",
            "Especialidade": "",
            "Campo de Aplicacao": "",
            "Local de Trabalho": "",
            "Quantidade de Handles": 0,
            "Handles": set()
        }
        
        # Coleta campos da tabela
        campos_map = {
            "Titulo": "Título",
            "Local de Trabalho": "Lugar de trabajo",
            "Campo de Aplicacao": "Campo de aplicación",
            "Especialidade": "Especialidad",
            "Grado": "Grado"
        }
        
        for coluna, campo in campos_map.items():
            try:
                elem = driver.find_element(By.XPATH, f"//td[contains(text(), '{campo}')]/following-sibling::td")
                autor[coluna] = elem.text.strip()
            except NoSuchElementException:
                pass
        
        # Coleta handles (publicações)
        while True:
            try:
                pubs = driver.find_elements(By.XPATH, "//a[contains(@href, '/handle/11336/')]")
                if not pubs:
                    break
                
                for pub in pubs:
                    href = pub.get_attribute("href")
                    if href:
                        handle = href.split("/handle/11336/")[-1]
                        if handle:
                            autor["Handles"].add(handle)
                
                try:
                    next_btn = driver.find_element(By.XPATH, "//a[@class='next-page-link' and contains(text(), 'Página siguiente')]")
                    next_btn.click()
                    WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/handle/11336/')]"))
                    )
                except NoSuchElementException:
                    break
            except Exception:
                break
        
        autor["Quantidade de Handles"] = len(autor["Handles"])
        autor["Handles"] = "|".join(sorted(autor["Handles"]))
        
        # Retorna sempre, mesmo sem publicações
        return autor
        
    except Exception as e:
        log_line(f"ERRO ao coletar {nome}: {e}")
        log_error(link, e)
        return None

# ----------------- Main -----------------
def main(reset=False):
    log_line("INICIO: coleta unificada")
    
    if reset:
        for f in [STATE_FILE, CSV_FILE, ERROR_FILE, PREVISAO_FILE, PARQUET_FILE]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                    log_line(f"RESET: removido {f}")
                except Exception as e:
                    log_line(f"RESET: falha ao remover {f}: {e}")
    
    estado = carregar_estado()
    offset = estado.get("ultimo_offset", 0)
    processados = estado.get("processados", {})
    
    total = obter_total_autores()
    if total is None:
        log_line("AVISO: não foi possível detectar automaticamente o total de autores")
        log_line("Você pode:")
        log_line("  1. Aguardar alguns minutos e tentar novamente (servidor pode estar instável)")
        log_line("  2. Informar o total manualmente editando o código (linha com 'total = ...')")
        log_line("  3. Usar um valor estimado (última coleta: 313483 autores)")
        
        # Usa total do estado anterior ou valor padrão
        if estado.get("total_autores", 0) > 0:
            total = estado["total_autores"]
            log_line(f"USANDO total do estado anterior: {total} autores")
        else:
            # Valor padrão baseado na última execução
            total = 313483
            log_line(f"USANDO total estimado: {total} autores (baseado em coleta anterior)")
            log_line("  O script continuará normalmente e coletará todos os autores disponíveis")
    
    total_pages = (total // PAGE_SIZE) + (1 if total % PAGE_SIZE else 0)
    log_line(f"TOTAL: {total} autores em ~{total_pages} páginas")
    
    # Previsão inicial de tempo
    tempo_por_autor_estimado = WAIT_SELENIUM + 2  # 2s de processamento + espera
    tempo_total_estimado_segundos = total * tempo_por_autor_estimado
    tempo_total_horas = tempo_total_estimado_segundos / 3600
    tempo_total_dias = tempo_total_horas / 24
    
    termino_previsto_utc = datetime.utcnow() + timedelta(seconds=tempo_total_estimado_segundos)
    termino_previsto_local = termino_previsto_utc + timedelta(hours=TZ_OFFSET)
    
    log_line("=" * 70)
    log_line("PREVISÃO INICIAL (estimativa conservadora):")
    log_line(f"  • Tempo por autor: ~{tempo_por_autor_estimado}s")
    log_line(f"  • Tempo total estimado: {tempo_total_horas:.1f} horas (~{tempo_total_dias:.1f} dias)")
    log_line(f"  • Início: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log_line(f"  • Término previsto: {termino_previsto_local.strftime('%Y-%m-%d %H:%M:%S')} UTC-3")
    log_line(f"  • Autores já processados: {len(processados)}")
    log_line(f"  • Autores restantes: {total - len(processados)}")
    if len(processados) > 0:
        tempo_restante = (total - len(processados)) * tempo_por_autor_estimado / 3600
        log_line(f"  • Tempo restante estimado: {tempo_restante:.1f} horas")
    log_line("=" * 70)
    
    initialize_csv()
    
    driver = None
    try:
        driver = configurar_driver()
        log_line("Selenium configurado com sucesso")
        
        start_time = time.time()
        autores_processados_count = len(processados)
        
        while offset <= total:
            pagina_atual = (offset // PAGE_SIZE) + 1
            log_line(f"PAGINA {pagina_atual}/{total_pages}: offset={offset}")
            
            # Obtém links da página
            autores_pagina = obter_links_pagina(offset)
            log_line(f"  Encontrados {len(autores_pagina)} autores na página")
            
            # Processa cada autor
            for autor_info in autores_pagina:
                link = autor_info["link"]
                nome = autor_info["nome"]
                
                # Pula se já foi processado
                if link in processados:
                    continue
                
                log_line(f"  Processando: {nome}")
                
                # Coleta dados detalhados
                dados = coletar_dados_autor(driver, nome, link)
                
                if dados:
                    append_csv_row(dados)
                    append_parquet_row(dados)  # Atualiza Parquet incrementalmente
                    processados[link] = True
                    autores_processados_count += 1
                    
                    if dados['Quantidade de Handles'] > 0:
                        log_line(f"    ✓ Salvo: {dados['Quantidade de Handles']} publicações")
                    else:
                        log_line(f"    ✓ Salvo: sem publicações")
                else:
                    log_line(f"    ✗ Erro ao processar")
                
                # Salva estado
                estado["ultimo_offset"] = offset
                estado["processados"] = processados
                estado["total_autores"] = total
                salvar_estado(estado)
                
                # Atualiza previsão
                write_previsao(offset, total, start_time, autores_processados_count)
                
                time.sleep(WAIT_SELENIUM)
            
            # Próxima página
            offset += PAGE_SIZE
            elapsed = time.time() - start_time
            log_line(f"PROGRESSO: {autores_processados_count} autores | {elapsed/3600:.2f}h decorridas")
            time.sleep(WAIT_SECONDS)
        
        log_line(f"FINAL: {autores_processados_count} autores processados")
        
        # Verifica se Parquet foi criado
        if os.path.exists(PARQUET_FILE):
            try:
                df = pd.read_parquet(PARQUET_FILE)
                log_line(f"PARQUET: arquivo final com {len(df)} linhas")
            except Exception as e:
                log_line(f"AVISO: erro ao verificar Parquet final: {e}")
        elif HAS_PYARROW:
            log_line("AVISO: Parquet não foi criado (nenhum autor processado)")
        else:
            log_line("INFO: pyarrow não instalado - Parquet não disponível")
        
    except Exception as e:
        log_line(f"ERRO_CRITICO: {e}")
    finally:
        if driver:
            driver.quit()
            log_line("Driver Selenium encerrado")
    
    log_line("FIM: execução concluída")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reiniciar do zero")
    args = parser.parse_args()
    main(reset=args.reset)