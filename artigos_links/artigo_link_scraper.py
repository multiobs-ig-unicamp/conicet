#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
conicet_selenium_local.py
Scraper de artigos (links -> detalhe autor) usando WebDriver LOCAL (Edge/Chrome).
Sem webdriver_manager; passe --driver-path se o driver não estiver no PATH.
"""

import os
import time
import argparse
import json
import csv
from datetime import datetime
import pandas as pd
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.service import Service as EdgeService

# ---------- CONFIG ----------
OUTPUT_DIR = "saida_arq_articulo_link"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LINKS_FILE = os.path.join(OUTPUT_DIR, "links_coletados.txt")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint_articulo_link.txt")
PARQUET_FILE = os.path.join(OUTPUT_DIR, "dados_completos_articulos_link.parquet")
ERRORS_FILE = os.path.join(OUTPUT_DIR, "erros_selenium.csv")

URL_BASE = "https://ri.conicet.gov.ar/discover?rpp=10&etal=0&group_by=none&page="
PAGE_LOAD_SLEEP = 3
HEADLESS = True
MAX_TENTATIVAS_PAGINA = 10
SLEEP_BETWEEN_PAGES = 3

# ---------- HELPERS ----------
def log(msg):
    ts = datetime.utcnow().isoformat()
    print(f"{ts} - {msg}")

def escapar_texto(texto):
    if texto is None:
        return ""
    return texto.replace('"', '""').replace("\n", " ").replace("\r", " ").strip()

def carregar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        except:
            return 1
    return 1

def salvar_checkpoint(pagina):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        f.write(str(pagina))

def carregar_links_existentes():
    s = set()
    if os.path.exists(LINKS_FILE):
        with open(LINKS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    s.add(line)
    return s

def salvar_links_novos(links, existing_set):
    if not links:
        return
    with open(LINKS_FILE, "a", encoding="utf-8") as f:
        for link in links:
            if link not in existing_set:
                f.write(link + "\n")
                existing_set.add(link)

def salvar_dado_parquet(dado, parquet_path=PARQUET_FILE):
    df_new = pd.DataFrame([dado])
    if os.path.exists(parquet_path):
        try:
            df_exist = pd.read_parquet(parquet_path)
            df = pd.concat([df_exist, df_new], ignore_index=True)
        except Exception:
            df = df_new
    else:
        df = df_new
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

def append_error(pagina_url, erro):
    row = {"pagina": pagina_url, "erro": str(erro), "ts": datetime.utcnow().isoformat()}
    write_header = not os.path.exists(ERRORS_FILE)
    with open(ERRORS_FILE, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["pagina", "erro", "ts"])
        if write_header:
            writer.writeheader()
        writer.writerow(row)

# ---------- WEBDRIVER ----------
def iniciar_driver_local(browser="edge", driver_path=None, headless=True):
    try:
        if browser.lower() == "edge":
            options = webdriver.EdgeOptions()
            options.use_chromium = True
            if headless:
                options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            if driver_path:
                if not os.path.exists(driver_path):
                    raise FileNotFoundError(f"Edge driver não encontrado: {driver_path}")
                service = EdgeService(executable_path=driver_path)
                driver = webdriver.Edge(service=service, options=options)
            else:
                driver = webdriver.Edge(options=options)

        elif browser.lower() == "chrome":
            options = webdriver.ChromeOptions()
            if headless:
                options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add.add_argument("--window-size=1920,1080")
            if driver_path:
                if not os.path.exists(driver_path):
                    raise FileNotFoundError(f"Chrome driver não encontrado: {driver_path}")
                service = ChromeService(executable_path=driver_path)
                driver = webdriver.Chrome(service=service, options=options)
            else:
                driver = webdriver.Chrome(options=options)
        else:
            raise ValueError("browser inválido, use 'edge' ou 'chrome'")

        return driver

    except Exception as e:
        msg = (
            "Falha ao iniciar o WebDriver local.\n"
            "Verifique driver/navegador.\n"
            f"Erro original: {e}"
        )
        raise RuntimeError(msg)

# ---------- EXTRAÇÃO ----------
def extrair_informacoes(driver, url):
    driver.get(url)
    time.sleep(PAGE_LOAD_SLEEP)
    dados = {"link": url, "author": ""}
    try:
        try:
            autor_elem = driver.find_element(By.XPATH, '//div[contains(@class,"simple-item-view-authors")]//a')
            dados["author"] = escapar_texto(autor_elem.text)
        except NoSuchElementException:
            dados["author"] = ""
    except Exception as e:
        append_error(url, e)
    return dados

# ---------- COLETA LINKS ----------
def coletar_links_da_pagina(driver, page):
    url = URL_BASE + str(page)
    driver.get(url)
    time.sleep(PAGE_LOAD_SLEEP)
    items = driver.find_elements(By.CLASS_NAME, "ds-artifact-item")
    links = []
    for item in items:
        try:
            link_elem = item.find_element(By.XPATH, './/a[contains(@href, "/handle/11336/")]')
            href = link_elem.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = "https://ri.conicet.gov.ar" + href
                links.append(href)
        except:
            continue
    return links

# ---------- MAIN ----------
def main(browser="edge", driver_path=None, start_page=None, end_page=None, headless=True):
    log("Iniciando coleta (driver local).")
    global HEADLESS
    HEADLESS = headless

    start = start_page if start_page is not None else carregar_checkpoint()
    if start < 1:
        start = 1
    existing_links = carregar_links_existentes()

    try:
        driver = iniciar_driver_local(browser=browser, driver_path=driver_path, headless=headless)
    except RuntimeError as e:
        log(str(e))
        return

    # ----------- detectar total de páginas automaticamente ------------
    if end_page is None:
        try:
            url0 = URL_BASE + "1"
            driver.get(url0)
            time.sleep(PAGE_LOAD_SLEEP)

            h2 = driver.find_element(By.CSS_SELECTOR, "h2.ds-div-head").text

            m = re.search(r"total de\s+([\d\.]+)", h2)
            if m:
                total_resultados = int(m.group(1).replace(".", ""))
                pagina_max = (total_resultados // 10) + (1 if total_resultados % 10 > 0 else 0)
                log(f"Total de resultados: {total_resultados}. Total de páginas: {pagina_max}")
            else:
                log("Não consegui identificar total de páginas. Fallback 27154.")
                pagina_max = 27154
        except Exception as e:
            log(f"Erro ao detectar total: {e}")
            pagina_max = 27154
    else:
        pagina_max = end_page

    # ---------------- LOOP PRINCIPAL -----------------
    try:
        page = start

        while page <= pagina_max:
            tentativa = 0
            success = False

            while tentativa < MAX_TENTATIVAS_PAGINA and not success:
                try:
                    log(f"Processando página {page}/{pagina_max}...")
                    links = coletar_links_da_pagina(driver, page)

                    if not links:
                        log(f"Nenhum item na página {page}. Encerrando (fim real).")
                        salvar_checkpoint(page)
                        return

                    salvar_links_novos(links, existing_links)

                    log(f"Encontrados {len(links)} links na página {page}.")

                    # detalhes
                    for l in links:
                        dados = extrair_informacoes(driver, l)
                        salvar_dado_parquet(dados)

                    salvar_checkpoint(page + 1)
                    success = True

                except WebDriverException as e:
                    tentativa += 1
                    append_error(URL_BASE + str(page), e)
                    log(f"WebDriverException página {page}, tentativa {tentativa}. Reiniciando driver.")
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(3)
                    driver = iniciar_driver_local(browser=browser, driver_path=driver_path, headless=headless)

                except Exception as e:
                    tentativa += 1
                    append_error(URL_BASE + str(page), e)
                    log(f"Erro inesperado página {page}, tentativa {tentativa}: {e}")
                    time.sleep(2)

            if not success:
                log(f"Falha definitiva página {page}. Avançando.")
                salvar_checkpoint(page + 1)

            time.sleep(SLEEP_BETWEEN_PAGES)
            page += 1

    finally:
        try:
            driver.quit()
        except:
            pass

    log("Coleta finalizada.")


# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=str, default="edge", choices=["edge", "chrome"])
    parser.add_argument("--driver-path", type=str, default=None)
    parser.add_argument("--start-page", type=int, default=None)
    parser.add_argument("--end-page", type=int, default=None)
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    main(browser=args.browser,
         driver_path=args.driver_path,
         start_page=args.start_page,
         end_page=args.end_page,
         headless=args.headless)
