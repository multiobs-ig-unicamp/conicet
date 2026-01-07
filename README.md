Coleta de Artigos – CONICET

Script em Python para coleta de metadados de artigos científicos a partir de links, utilizando Selenium e salvando os dados em formato Parquet.

Requisitos:
- Python 3.9+
- Chrome ou Edge
- Driver do navegador instalado
- Git

Clonar o repositório:
git clone https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git
cd SEU_REPOSITORIO

Criar ambiente virtual:
python3 -m venv .venv

Ativar ambiente virtual:
source .venv/bin/activate

Instalar dependências:
pip install -r requirements.txt

Executar o script:
python ./authors_data_scraper.py

Desativar ambiente virtual:
deactivate

Arquivos gerados:
- arq_articulos_authors/articulos.parquet
- arq_articulos_authors/logs/execucao_script.log
- arq_articulos_authors/execucao_checkpoint.txt

Os dados são salvos incrementalmente, após cada link processado.

Dependências:
selenium==4.25.0
pandas==2.2.2
pyarrow==15.0.2

.gitignore (criar arquivo .gitignore com o conteúdo abaixo):

.venv/
__pycache__/
*.pyc
*.log
arq_articulos_authors/articulos.parquet
arq_articulos_authors/logs/
arq_articulos_authors/execucao_checkpoint.txt

Licença:
Uso acadêmico.
