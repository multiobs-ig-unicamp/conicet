================================================
  Guia Rápido de Comandos para Projetos Python
================================================

---
1. Criar um novo projeto
---
# Vá para onde seus projetos ficam
cd Documentos/

# Crie a pasta do projeto e entre nela
mkdir nome_do_projeto
cd nome_do_projeto


---
2. Criar o arquivo de dependências
---
# Crie o arquivo requirements.txt com os pacotes necessários
# (Exemplo com pacotes comuns)
cat <<EOL > requirements.txt
oss2
requests
beautifulsoup4
tqdm
selenium
lxml
pandas
pyarrow
webdriver-manager==4.0.2


---
3. Criar e ativar o ambiente virtual
---
# Crie o ambiente (só precisa fazer uma vez por projeto)
python3 -m venv venv

# Ative o ambiente (faça isso toda vez que for trabalhar no projeto)
source venv/bin/activate

# (Seu terminal agora deve mostrar `(venv)` no início)


---
4. Instalar as dependências
---
# Instale todos os pacotes listados no requirements.txt
pip install -r requirements.txt


---
5. Executar seu script
---
# Use o comando 'python' simples
python seu_script.py


---
6. Sair do ambiente
---
# Quando terminar de trabalhar, desative o ambiente
deactivate


---
Comando Extra Útil
---
# Para salvar todos os pacotes instalados no ambiente para o requirements.txt
pip freeze > requirements.txt
