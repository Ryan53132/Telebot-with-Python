from fastapi import FastAPI
import telebot
import ollama
import threading
import uvicorn
import os
import sqlite3
import json
import logging
import tempfile
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
import whisper
import subprocess
import imageio_ffmpeg as ffmpeg

# ⚙️ Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ⚙️ Configuração inicial
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:4b")
BACKGROUND = os.getenv("BACKGROUND", os.getenv("Background", ""))
BANCO_SCHEMA = os.getenv("BANCO", os.getenv("banco", ""))

# 🔧 Configuração do ffmpeg para Whisper
FFMPEG_EXE = None
FFMPEG_DIR = None
try:
    FFMPEG_EXE = ffmpeg.get_ffmpeg_exe()
    FFMPEG_DIR = os.path.dirname(FFMPEG_EXE)
    # Converte para caminho absoluto
    FFMPEG_EXE = os.path.abspath(FFMPEG_EXE)
    FFMPEG_DIR = os.path.abspath(FFMPEG_DIR)
    
    if FFMPEG_DIR not in os.environ.get("PATH", ""):
        os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ.get("PATH", "")
        logger.info(f"ffmpeg adicionado ao PATH: {FFMPEG_DIR}")
    
    # Verifica se o ffmpeg está acessível
    test_process = subprocess.run(
        [FFMPEG_EXE, "-version"],
        capture_output=True,
        timeout=5
    )
    if test_process.returncode == 0:
        logger.info(f"ffmpeg verificado e funcionando: {FFMPEG_EXE}")
    else:
        logger.warning(f"ffmpeg encontrado mas pode não estar funcionando corretamente")
except Exception as e:
    logger.warning(f"Erro ao configurar ffmpeg: {e}")

# Validação de variáveis obrigatórias
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN não encontrado nas variáveis de ambiente!")

# 🚀 Inicializa serviços
try:
    model = whisper.load_model("tiny")
    logger.info("Modelo Whisper carregado com sucesso")
except Exception as e:
    logger.error(f"Erro ao carregar modelo Whisper: {e}")
    raise

app = FastAPI(title="Servidor Ollama Tools + Bot Telegram")
bot = telebot.TeleBot(TELEGRAM_TOKEN)

# 💾 Banco de dados
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

def seguro_json(obj: Any) -> Dict[str, Any]:
    """Garante que o resultado sempre vire um dicionário seguro."""
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except json.JSONDecodeError:
            return {"resposta": obj}
    return {"resposta": str(obj)}


def executar_sql(query: str) -> Dict[str, Any]:
    """Executa uma query SQL e retorna o resultado."""
    if not query or not query.strip():
        return {"erro": "Query vazia"}
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            query_upper = query.strip().upper()
            
            if query_upper.startswith("SELECT"):
                cur.execute(query)
                resultado = cur.fetchall()
                colunas = [desc[0] for desc in cur.description] if cur.description else []
                return {"query": query, "colunas": colunas, "resultado": resultado}
            else:
                cur.execute(query)
                conn.commit()
                return {"query": query, "status": "Executado com sucesso"}
    except sqlite3.Error as e:
        logger.error(f"Erro SQL: {e}")
        return {"erro": f"Erro ao executar SQL: {str(e)}", "query": query}
    except Exception as e:
        logger.error(f"Erro inesperado ao executar SQL: {e}")
        return {"erro": f"Erro inesperado: {str(e)}", "query": query}


def formatar_resultado(resultado: List[Any], colunas: Optional[List[str]] = None) -> str:
    """Formata o resultado da consulta em tabela Markdown."""
    if not resultado:
        return "Nenhum resultado encontrado."

    LIMITE_RESULTADOS = 10

    if isinstance(resultado[0], dict):
        colunas = list(resultado[0].keys())
        linhas = [[str(item.get(c, "")) for c in colunas] for item in resultado]
    else:
        if not colunas:
            colunas = [f"col{i+1}" for i in range(len(resultado[0]))]
        linhas = [[str(c) for c in linha] for linha in resultado]

    truncado = len(linhas) > LIMITE_RESULTADOS
    if truncado:
        linhas = linhas[:LIMITE_RESULTADOS]

    # Escapa pipes nas células para evitar quebra de formatação
    def escape_cell(cell: str) -> str:
        return cell.replace("|", "\\|").replace("\n", " ")

    tabela = "| " + " | ".join(escape_cell(c) for c in colunas) + " |\n"
    tabela += "| " + " | ".join(["---"] * len(colunas)) + " |\n"
    for linha in linhas:
        tabela += "| " + " | ".join(escape_cell(str(c)) for c in linha) + " |\n"

    if truncado:
        tabela += f"\n⚠️ Mostrando apenas os primeiros {LIMITE_RESULTADOS} resultados."

    return tabela


# ======================================================
# 🧩 FUNÇÕES TOOL (executadas quando o modelo decide)
# ======================================================

def AI_SQL(pergunta: str) -> Dict[str, Any]:
    """Converte linguagem natural em SQL via Ollama e executa a query."""
    if not BANCO_SCHEMA:
        logger.warning("BANCO_SCHEMA não configurado, usando contexto padrão")
    
    try:
        resposta = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": BANCO_SCHEMA or "Você é um assistente SQL especializado."},
                {"role": "user", "content": pergunta}
            ]
        )
        conteudo = resposta["message"]["content"]
        logger.info(f"Resposta Ollama recebida para pergunta: {pergunta[:50]}...")

        # Extrai SQL de blocos de código markdown
        if "```" in conteudo:
            partes = conteudo.split("```")
            for p in partes:
                p_upper = p.upper()
                if any(x in p_upper for x in ["SELECT", "INSERT", "UPDATE", "DELETE"]):
                    sql_query = p.replace("sql", "").replace("SQL", "").strip()
                    logger.info(f"SQL extraído: {sql_query}")
                    return executar_sql(sql_query)
        
        # Se não encontrou SQL em blocos, tenta usar o conteúdo direto
        sql_query = conteudo.strip()
        logger.info(f"SQL extraído (direto): {sql_query}")
        return executar_sql(sql_query)
    
    except Exception as e:
        logger.error(f"Erro em AI_SQL: {e}")
        return {"erro": f"Erro ao processar pergunta: {str(e)}"}

# ======================================================
# 🔧 DEFINIÇÃO DAS TOOLS
# ======================================================

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "ExecSql",
            "description": "Formata uma query SQL para ser executada no banco de dados",
            "parameters": {
                "type": "object",
                "properties": {"pergunta": {"type": "string"}},
                "required": ["pergunta"]
            }
        }
    }
]


# ======================================================
# 🧠 PROCESSAMENTO COM TOOLS (Ollama decide automaticamente)
# ======================================================

def processar_com_tools(pergunta: str) -> Dict[str, Any]:
    """Processa a pergunta com o Ollama e executa tool automaticamente."""
    messages = [
        {"role": "system", "content": BACKGROUND or "Você é um assistente útil."},
        {"role": "user", "content": pergunta}
    ]

    resposta = ""
    try:
        for chunk in ollama.chat(model=OLLAMA_MODEL, messages=messages, tools=TOOLS, stream=True):
            if "message" in chunk and "content" in chunk["message"]:
                resposta += chunk["message"]["content"]
            if "message" in chunk and "tool_calls" in chunk["message"]:
                for call in chunk["message"]["tool_calls"]:
                    nome = call["function"]["name"]
                    args = call["function"].get("arguments", "{}")
                    if isinstance(args, str):
                        try:
                            args = json.loads(args)
                        except json.JSONDecodeError:
                            logger.error(f"Erro ao decodificar argumentos JSON: {args}")
                            continue

                    if nome == "ExecSql":
                        pergunta_sql = args.get("pergunta", "")
                        if not pergunta_sql:
                            return {"erro": "Pergunta não fornecida para ExecSql"}
                        return AI_SQL(pergunta_sql)

        return {"resposta": resposta.strip() if resposta.strip() else "Sem resposta gerada"}
    
    except Exception as e:
        logger.error(f"Erro em processar_com_tools: {e}")
        return {"erro": f"Erro ao processar: {str(e)}"}

# ======================================================
# 🤖 BOT TELEGRAM
# ======================================================

@bot.message_handler(commands=["start"])
def start_cmd(msg):
    bot.reply_to(msg, (
        "Olá! Sou a Lola 🙅‍♀️.\n"
        "Posso gerenciar seu banco de dados via IA.\n"
        "Use /sql seguido de um comando, por exemplo:\n"
        "👉 /sql listar todos os usuários maiores de 18 anos."
    ))


@bot.message_handler(commands=["sql"])
def sql_cmd(msg):
    pergunta = msg.text.replace("/sql", "").strip()
    if not pergunta:
        bot.reply_to(msg, "Envie algo como: `/sql quantos usuários existem?`", parse_mode="Markdown")
        return

    try:
        resultado = seguro_json(processar_com_tools(pergunta))
        logger.info(f"Resultado processado para usuário {msg.from_user.id}: {type(resultado)}")

        # Caso o resultado seja um dicionário retornado pelo executar_sql
        if isinstance(resultado, dict):
            if "erro" in resultado:
                bot.reply_to(msg, f"❌ Erro: {resultado['erro']}")
            elif "resultado" in resultado:
                tabela = formatar_resultado(resultado["resultado"], resultado.get("colunas"))
                bot.reply_to(
                    msg,
                    f"✅ *Query:* `{resultado.get('query', '')}`\n\n📊 *Resultado:*\n{tabela}",
                    parse_mode="Markdown"
                )
            elif "status" in resultado:
                bot.reply_to(
                    msg,
                    f"✅ {resultado['status']}\nQuery: `{resultado.get('query', '')}`",
                    parse_mode="Markdown"
                )
            elif "resposta" in resultado:
                bot.reply_to(msg, resultado["resposta"])
            else:
                logger.warning(f"Retorno inesperado: {resultado}")
                bot.reply_to(msg, f"ℹ️ Retorno inesperado: {resultado}")

        elif isinstance(resultado, str):
            bot.reply_to(msg, resultado)
        else:
            logger.warning(f"Tipo de retorno não reconhecido: {type(resultado)}")
            bot.reply_to(msg, f"⚠️ Tipo de retorno não reconhecido: {type(resultado)}")

    except Exception as e:
        logger.error(f"Erro em sql_cmd: {e}", exc_info=True)
        bot.reply_to(msg, f"❌ Erro: {e}")



@bot.message_handler(content_types=['voice'])
def handle_voice(msg):
    ogg_path = None
    wav_path = None
    
    try:
        # 🔹 Baixa o arquivo do Telegram
        file_info = bot.get_file(msg.voice.file_id)
        downloaded_file = bot.download_file(file_info.file_path)

        # 🔹 Cria arquivos temporários
        with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp_ogg:
            ogg_path = tmp_ogg.name
            tmp_ogg.write(downloaded_file)

        wav_path = ogg_path.replace('.ogg', '.wav')

        # 🔹 Caminho do ffmpeg dentro do venv
        ffmpeg_path = ffmpeg.get_ffmpeg_exe()

        command = [
            ffmpeg_path,
            '-y',
            '-i', ogg_path,
            '-ac', '1',
            '-ar', '16000',
            wav_path
        ]

        # 🔹 Executa ffmpeg
        process = subprocess.run(
            command,
            capture_output=True,
            text=True
        )

        # 🔹 Verifica saída e erros
        if process.returncode != 0:
            logger.error(f"Erro ao executar ffmpeg: {process.stderr}")
            bot.reply_to(msg, f"❌ Erro ao converter áudio:\n{process.stderr[:200]}")
            return

        logger.info("Conversão de áudio concluída com sucesso")

        # 🔹 Garante que o ffmpeg está no PATH antes de usar o Whisper
        if FFMPEG_DIR and FFMPEG_DIR not in os.environ.get("PATH", ""):
            os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ.get("PATH", "")
            logger.info(f"ffmpeg adicionado ao PATH para Whisper: {FFMPEG_DIR}")
        
        # Verifica se o arquivo WAV existe e tem tamanho válido
        if not os.path.exists(wav_path) or os.path.getsize(wav_path) == 0:
            logger.error(f"Arquivo WAV inválido ou vazio: {wav_path}")
            bot.reply_to(msg, "❌ Erro: Arquivo de áudio não foi criado corretamente.")
            return

        # 🔹 Transcreve o WAV com Whisper
        try:
            logger.info(f"Iniciando transcrição do arquivo: {wav_path}")
            result = model.transcribe(wav_path, fp16=False)
            texto = result["text"].strip()
            logger.info("Transcrição concluída com sucesso")

            if not texto:
                bot.reply_to(msg, "⚠️ Não foi possível transcrever o áudio (áudio vazio ou sem fala).")
            else:
                # 🔹 Responde a transcrição
                bot.reply_to(msg, f"🗣️ Transcrição: {texto}")
        except FileNotFoundError as e:
            logger.error(f"WinError 2 - Arquivo não encontrado: {e}")
            logger.error(f"ffmpeg_path: {FFMPEG_EXE if FFMPEG_EXE else 'N/A'}")
            logger.error(f"ffmpeg_dir: {FFMPEG_DIR if FFMPEG_DIR else 'N/A'}")
            logger.error(f"PATH atual (primeiros 300 chars): {os.environ.get('PATH', '')[:300]}...")
            logger.error(f"Arquivo WAV existe: {os.path.exists(wav_path) if wav_path else 'N/A'}")
            bot.reply_to(msg, "❌ Erro: ffmpeg não encontrado. O Whisper precisa do ffmpeg no PATH do sistema.")
        except Exception as e:
            logger.error(f"Erro ao transcrever com Whisper: {e}", exc_info=True)
            bot.reply_to(msg, f"❌ Erro ao transcrever áudio: {e}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Erro FFmpeg (CalledProcessError): {e}")
        error_msg = e.stderr if hasattr(e, 'stderr') and e.stderr else str(e)
        bot.reply_to(msg, f"❌ Erro FFmpeg:\n{error_msg[:200]}")
    except Exception as e:
        logger.error(f"Erro ao processar áudio: {e}", exc_info=True)
        bot.reply_to(msg, f"❌ Erro ao processar áudio: {e}")
    finally:
        # 🔹 Limpa os arquivos temporários
        for path in [ogg_path, wav_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logger.warning(f"Erro ao remover arquivo temporário {path}: {e}")



# ======================================================
# 🏁 EXECUÇÃO COM UVICORN + THREAD DO BOT
# ======================================================

def run_bot():
    try:
        bot.polling(non_stop=True, interval=0, timeout=20)
    except Exception as e:
        logger.error(f"Erro no bot do Telegram: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        # Inicia o bot em thread separada
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()      
        # Inicia o servidor FastAPI
        uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True, reload_dirs=["."])
    except KeyboardInterrupt:
        logger.info("Servidor encerrado pelo usuário")
    except Exception as e:
        logger.error(f"Erro ao iniciar servidor: {e}", exc_info=True)
