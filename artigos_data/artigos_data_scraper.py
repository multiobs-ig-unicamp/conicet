#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import logging
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.service import Service as EdgeService

# -------------------------------------------------------------------------
# CONFIGURAÇÕES
# -------------------------------------------------------------------------

LOG_DIR = "saida_arq_articulos_authors/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "execucao_script.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

PARQUET_FILE = "arq_articulos_authors/articulos.parquet"
CHECKPOINT_FILE = "arq_articulos_authors/execucao_checkpoint.txt"
LINKS_FILE = "saida_arq_articulo_link/links_coletados.txt"

# -------------------------------------------------------------------------
# FUNÇÃO PARA INICIAR DRIVER LOCAL (sem webdriver_manager)
# -------------------------------------------------------------------------

def iniciar_driver_local(browser="edge", driver_path=None, headless=False):
    if browser.lower() == "edge":
        options = webdriver.EdgeOptions()
        options.use_chromium = True
        if headless:
            options.add_argument("--headless=new")

        if driver_path:
            service = EdgeService(executable_path=driver_path)
            driver = webdriver.Edge(service=service, options=options)
        else:
            driver = webdriver.Edge(options=options)

    elif browser.lower() == "chrome":
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")

        if driver_path:
            service = ChromeService(executable_path=driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)

    else:
        raise ValueError("Browser inválido (use 'chrome' ou 'edge').")

    return driver

# -------------------------------------------------------------------------
# UTILITÁRIOS
# -------------------------------------------------------------------------

def escapar_texto(texto):
    if texto:
        texto = texto.replace('"', '""')
        texto = texto.replace('\n', ' ').replace('\r', ' ')
    return texto

def salvar_dados(dados, arquivo_parquet):
    os.makedirs(os.path.dirname(arquivo_parquet), exist_ok=True)

    df_new = pd.DataFrame([dados])

    if os.path.exists(arquivo_parquet):
        try:
            df_exist = pd.read_parquet(arquivo_parquet)
            df = pd.concat([df_exist, df_new], ignore_index=True)
        except:
            df = df_new
    else:
        df = df_new

    df.to_parquet(arquivo_parquet, engine="pyarrow", index=False)

def salvar_checkpoint(path, lista):
    with open(path, "w") as f:
        f.write("\n".join(lista))

# -------------------------------------------------------------------------
# EXTRAÇÃO DE INFORMAÇÕES DO ARTIGO
# -------------------------------------------------------------------------

def extrair_informacoes(driver, url):
    driver.get(url)
    time.sleep(3)

    dados = {"url": url}

    def safe_xpath(xpath, attr="text", multi=False):
        try:
            if multi:
                elems = driver.find_elements(By.XPATH, xpath)
                return "; ".join([escapar_texto(e.text) for e in elems])
            elem = driver.find_element(By.XPATH, xpath)
            return escapar_texto(elem.get_attribute(attr) if attr != "text" else elem.text)
        except:
            return ""

    dados["Titulo"] = safe_xpath('//h1[@style="font-size:150%;font-weight: 500;font-family: \'Roboto\'; margin-top: 3px;"]')
    dados["Autores"] = safe_xpath('//div[@class="simple-item-view-authors"]//a', multi=True)
    dados["Data de Publicacao"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "Fecha de publicación:")]/following-sibling::span')
    dados["Editorial"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "Editorial:")]/following-sibling::span')
    dados["Revista"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "Revista:")]/following-sibling::span')
    dados["ISSN"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "ISSN:")]/following-sibling::span')
    dados["e-ISSN"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "e-ISSN:")]/following-sibling::span')
    dados["ISBN"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "ISBN:")]/following-sibling::span')
    dados["Idioma"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "Idioma:")]/following-sibling::span')
    dados["Tipo de Recurso"] = safe_xpath('//div[@class="simple-item-view-other"]/span[contains(text(), "Tipo de recurso:")]/following-sibling::span')
    dados["Resumo"] = safe_xpath('//div[@class="simple-item-view-description"]//div[@style="overflow-wrap: break-word;"]')
    dados["Palavras-chave"] = safe_xpath('//div[@class="simple-item-view-description"]//a[contains(@href, "/discover?filtertype=subject")]', multi=True)
    dados["URI"] = safe_xpath('//span[contains(text(), "URI:")]/following-sibling::a', attr="href")
    dados["URL_1"] = safe_xpath('(//span[contains(text(), "URL:")]/following-sibling::a)[1]', attr="href")
    dados["URL_2"] = safe_xpath('(//span[contains(text(), "URL:")]/following-sibling::a)[2]', attr="href")
    dados["DOI"] = safe_xpath('//span[contains(text(), "DOI:")]/following-sibling::a', attr="href")
    dados["dc_identifier"] = safe_xpath('//meta[@name="DC.identifier"]', attr="content")
    dados["metadata"] = safe_xpath('//div[@class="item-summary-view-metadata"]')

    return dados

# -------------------------------------------------------------------------
# PROCESSAMENTO PRINCIPAL
# -------------------------------------------------------------------------

def processar_links(driver, links):
    logging.info("Iniciando processamento dos links.")
    processados = []

    for link in links:
        try:
            logging.info(f"Processando: {link}")
            dados = extrair_informacoes(driver, link)
            salvar_dados(dados, PARQUET_FILE)
            processados.append(link)
        except Exception as e:
            logging.error(f"Erro no link {link}: {e}")
            time.sleep(2)

    return processados

# -------------------------------------------------------------------------
# EXECUÇÃO
# -------------------------------------------------------------------------

if __name__ == "__main__":
    # --- INICIA DRIVER LOCAL (como o outro script) ---
    driver = iniciar_driver_local(browser="edge", driver_path=None, headless=False)

    # --- CARREGA LINKS ---
    if not os.path.exists(LINKS_FILE):
        print("Arquivo de links não encontrado:", LINKS_FILE)
        exit()

    with open(LINKS_FILE, "r") as f:
        links = f.read().splitlines()

    # --- LIMITA PROCESSAMENTO (você muda aqui) ---
    #links_a_processar = links[:20]
    links_a_processar = links

    # --- PROCESSA ---
    processados = processar_links(driver, links_a_processar)

    # --- CHECKPOINT ---
    salvar_checkpoint(CHECKPOINT_FILE, processados)

    driver.quit()

    print("Execução concluída.")
    logging.info("Execução concluída.")
