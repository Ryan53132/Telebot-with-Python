from fastapi import FastAPI, Request
from fastmcp import FastMCP
import telebot
import ollama
import threading
import uvicorn
import os
import sqlite3
import requests
from dotenv import load_dotenv

# ⚙️ Configuração inicial
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:0.6b")
BACKGROUND = os.getenv("Background")
BACKGROUNDSuporte = os.getenv("BackgroundSuporte")

# 🚀 Inicializa serviços
app = FastAPI(title="API com FastAPI, MCP, Ollama e Telegram")
mcp = FastMCP(app)
bot = telebot.TeleBot(TELEGRAM_TOKEN)

# 💾 Criação do banco de dados
DB_PATH = "Teste.db"
with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teste (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Nome TEXT,
            Idade INTEGER
        )
    """)
    conn.commit()

# ======================================================
# 🧠 FUNÇÕES BASE
# ======================================================

def AI_SQL(pergunta: str) -> str:
    """Usa o Ollama para gerar resposta SQL"""
    resposta = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[{"role": "user", "content": BACKGROUND + pergunta}]
    )
    return resposta["message"]["content"]

def format_query(resposta: str) -> str:
    """Extrai SQL de blocos ```sql```"""
    try:
        partes = resposta.split("```")
        for parte in partes:
            if "SELECT" in parte or "INSERT" in parte or "UPDATE" in parte or "DELETE" in parte:
                return parte.replace("sql", "").strip()
    except Exception:
        return resposta
    return resposta

def formatar_resultado(resultado, colunas=None):
    """
    Formata o resultado da consulta em uma tabela Markdown.
    - resultado: lista de listas ou lista de dicionários
    - colunas: lista com nomes das colunas (opcional)
    """
    if not resultado:
        return "Nenhum resultado encontrado."

    # Caso o resultado seja lista de dicionários
    if isinstance(resultado[0], dict):
        colunas = list(resultado[0].keys())
        linhas = [[str(item.get(c, "")) for c in colunas] for item in resultado]
    else:
        # Gera nomes genéricos se não houver colunas
        if not colunas:
            colunas = [f"col{i+1}" for i in range(len(resultado[0]))]
        linhas = [[str(c) for c in linha] for linha in resultado]

    # Limita a 10 registros para evitar mensagens enormes
    limite = 10
    if len(linhas) > limite:
        linhas = linhas[:limite]
        truncado = True
    else:
        truncado = False

    # Monta a tabela Markdown
    tabela = "| " + " | ".join(colunas) + " |\n"
    tabela += "| " + " | ".join(["---"] * len(colunas)) + " |\n"
    for linha in linhas:
        tabela += "| " + " | ".join(linha) + " |\n"

    if truncado:
        tabela += "\n⚠️ Mostrando apenas os primeiros 10 resultados."

    return tabela

# ======================================================
# 🧩 ENDPOINTS HTTP
# ======================================================

@app.get("/")
async def home():
    return {"mensagem": "Servidor MCP + Ollama + Bot ativo e pronto!"}


@app.post("/executar")
async def executar_query(request: Request):
    """Executa queries SQL geradas pela IA"""
    data = await request.json()
    pergunta = data.get("pergunta")

    sql_query = format_query(AI_SQL(pergunta))
    print(f"[SQL GERADA] {sql_query}")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            if sql_query.strip().upper().startswith("SELECT"):
                cur.execute(sql_query)
                resultado = cur.fetchall()
                colunas = [desc[0] for desc in cur.description] if cur.description else []
                return {"query": sql_query, "colunas": colunas, "resultado": resultado}
            else:
                cur.execute(sql_query)
                conn.commit()
                return {"query": sql_query, "status": "Executado com sucesso"}
    except Exception as e:
        return {"erro": str(e), "query": sql_query}


# ======================================================
# 🧩 TOOLS MCP
# ======================================================

@mcp.tool(name="Consultar", description="Consulta registros no banco de dados via IA")
def consultar(pergunta: str):
    sql_query = format_query(AI_SQL(pergunta))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
    return f"Query executada com sucesso: {sql_query}"

@mcp.tool(name="Adicionar", description="Insere novos registros no banco de dados via IA")
def adicionar(pergunta: str):
    sql_query = format_query(AI(pergunta))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
    return f"Query executada com sucesso: {sql_query}"

@mcp.tool(name="Atualizar", description="Atualiza registros existentes via IA")
def atualizar(pergunta: str):
    sql_query = format_query(AI_SQL(pergunta))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
    return f"Query executada com sucesso: {sql_query}"

@mcp.tool(name="Deletar", description="Remove registros via IA")
def deletar(pergunta: str):
    sql_query = format_query(AI_SQL(pergunta))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
    return f"Query executada com sucesso: {sql_query}"

# ======================================================
# 🤖 BOT DO TELEGRAM (usa endpoints HTTP)
# ======================================================

BASE_URL = "http://127.0.0.1:8000"  # endereço local da API

@bot.message_handler(commands=["start"])
def start_cmd(msg):
    bot.reply_to(msg, "Olá! Sou a Lola 🙅‍♀️. Posso gerenciar o banco de dados.\nUse /sql <sua ação> para interagir comigo;"
    " use /help para ver os comandos disponíveis."
    " use /suporte para obter ajuda.")

@bot.message_handler(commands=["help"])
def help_cmd(msg):
    bot.reply_to(msg, "Comandos disponíveis:\n/sql <sua ação> - Executa uma ação no banco de dados.")

@bot.message_handler(commands=["dev"])
def consultar_cmd(msg):
    res = AI(msg.text.replace("/dev", "").strip())
    bot.reply_to(msg, res)

@bot.message_handler(commands=["sql"])
def sql_cmd(msg):
    """O bot envia a pergunta para o endpoint /executar"""
    pergunta = msg.text.replace("/sql", "").strip()
    if not pergunta:
        bot.reply_to(msg, "Envie uma pergunta sobre o banco de dados após o comando /sql.")
        return

    try:
        resposta = requests.post(f"{BASE_URL}/executar", json={"pergunta": pergunta})
        dados = resposta.json()
        if "erro" in dados:
            bot.reply_to(msg, f"Erro ao executar: {dados['erro']}")
        elif "resultado" in dados:
            resultado_formatado = formatar_resultado(dados["resultado"], dados.get("colunas"))
            resposta_final = (
            f"✅ *Query executada com sucesso!*\n\n"
            f"📜 *Query:*\n`{dados['query']}`\n\n"
            f"📊 *Resultado:*\n{resultado_formatado}")
            bot.reply_to(msg, resposta_final, parse_mode="Markdown")
        else:
            bot.reply_to(msg, f"✅ {dados.get('status', 'Ação concluída')}\nQuery: {dados['query']}")
    except Exception as e:
        bot.reply_to(msg, f"Erro ao comunicar com a API: {e}")

# ======================================================
# 🏁 EXECUÇÃO
# ======================================================

def run_bot():
    bot.polling(non_stop=True)

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
