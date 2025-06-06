# Validador de entidades
[![Licença](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![Linguagem](https://img.shields.io/badge/Language-Python-blue.svg)
![Framework](https://img.shields.io/badge/Framework-Flask-brightgreen.svg)
![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-orange.svg)

> Validador de conjunto de dados do Modelo BrCris

## Visão Geral

Esta seção deve fornecer uma visão geral mais detalhada da sua API.

* **Funcionalidades Principais:**
    * Pat entes.
    * Marcas.

* **Tecnologias Utilizadas:**
    * Python 3.10.8
    * Flask

* **Formato dos Dados:** 
    * JSON


## Configurações de ambiente de desnevolvimento

1. Criar ambiente virtual

```bash
    python -m venv venv
```

2. Ativar ambiente virtual

```bash
    .\venv\Scripts\Activate # No Windows

    source venv/bin/activate  # No Linux/macOS
```

3. Instalar todas as dependencias do projeto

```bash
    pip install -r requirements.txt
```

4. Atualizar arquivo de dependencias do projeto

```bash
    pip freeze > requirements.txt
```

5.  Configure as variáveis de ambiente :
    * Crie um arquivo `.env` com as configurações necessárias 
    * Exemplo de `.env`:
        ```
        DATABASE_URL=postgresql://usuario:senha@host:porta/banco
        API_KEY=sua_chave_secreta
        ```
5.  Execute a API:
    ```bash
    python app.py  

    streamlit run app.py
    ```