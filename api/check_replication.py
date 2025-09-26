# -*- coding: utf-8 -*-
import os
import psycopg2
from flask import Flask, jsonify, request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# A Vercel exige que a aplicação Flask se chame 'app'
app = Flask(__name__)

# --- Dicionários de Configuração ---
EMPRESAS_POR_PORTA = {
    21001: "CENTER MALHAS", 21002: "SPEED COPIAS", 21003: "MACO MATERIAIS",
    21004: "NUTRI UNIÃO UVA", 21005: "NUTRI UNIÃO PU", 21006: "ATACADÃO MATERIAIS DE CONSTRUÇÃO",
    21007: "PIRAMIDE AUTOPECAS", 21008: "REBRAS REC. DE PAPEL BRAS", 21009: "DISTRIBUIDORA GRANDE RIO",
    21010: "NOVA AGROPECUARIA", 21011: "CRIATIVE INFORMATICA", 21012: "TECNOHOUSE INFORMATICA",
    21013: "CASA DE CARNES ROSSA", 21014: "RETIVALLE RETIFICA", 21015: "SPECIALE PORTAS",
    21016: "MD GRAZZIOTIN MATERIAIS ELETRICOS LTDA", 21017: "MOTO PECAS DOS COBRAS", 21018: "LUBRIFICAR",
    21019: "EMPRESA PENDENTE 21019",
    21020: "ROSSA INDUSTRIA E COMERCIO DE CARNES", 21021: "CANTINHO DA CRIANCA", 21022: "SERGIO ANDRUKIU/LUKITO",
    21023: "MADEIREIRA WTK", 21024: "AGRO VALE - 5253", 21025: "AGRO VALE - 5254",
    21026: "RESTAURANTE E LANCHONETE ROSSA", 21027: "TOCA DO JAVALI", 21028: "JAVALI ARMAS",
    21029: "EMPRESA PENDENTE 21029",
    21030: "EMPRESA PENDENTE 21030",
    21031: "DALGALLO MUDAS FLORESTAIS", 21032: "AUTO TINTAS AUTOMOTIVAS", 21033: "AUTO TINTAS FILIAL",
    21034: "GALENICA MANIPULACAO", 21035: "CENTRAL DO GESSO", 21036: "VIDRACARIA AVENIDA",
    21037: "EMPRESA PENDENTE 21037",
    21038: "RECANTO BELA VISTA", 21039: "PONTO DA MODA", 21040: "MS COMERCIO DE PNEUS",
    21041: "MS MANUTENCAO DE VEICULOS", 21042: "MERCADO BOM GOSTO", 21043: "AGRO TICO",
    21044: "RESTAURANTE MACIEL", 21045: "AGRO VALDIR FILIAL", 21046: "CHOCOLATE E CIA",
    21047: "LOJA BIASI", 21048: "LACHMAN COMERCIO DE FRUTAS LTDA", 21049: "DINOS SPORT UVA",
    21050: "AEB DEPARTAMENTO DE MODA", 21051: "NEUMA FLORES E DECORACOES", 21052: "CANTINHO DOS PRESENTES",
    21053: "MERCADO IGUAÇU IPV6", 21054: "MERCADO DO PORTO IPV6", 21055: "LOJA CRISTINA IPV6",
    21056: "GW ELETRONICA", 21057: "GIRASSOL JARDINAGENS", 21058: "AUTO ELETRICA PASA IPV6",
    21059: "DINOS PU - IPV6", 21060: "DINOS MODA ESPORTIVA", 21061: "KASCHUK BAR E LANCHONETE LTDA",
    21062: "PEG MATERIAIS ELETRICOS LTDA - IPV6", 21063: "MARQUINHOS AUTOVIDROS - IPV6",
    21064: "PANIFICADORA E CONFEITARIA SUPERPAO - IPV6", 21065: "AGROPECUARIA DO ALEMAO",
    21066: "LOJA SANDY - IPV6", 21067: "BRINCADEIRA DE PAPEL", 21068: "CASA DOS OCULOS - IPV6",
    21069: "CARLAO MODA MIX - IPV6", 21070: "CEPAVEL - IPV6", 21071: "MM CELL PU - IPV6",
    21072: "MARLENE NHOATO PEREIRA - IPV6", 21073: "SMART CONCEPT UVA", 21074: "GALO GÁS - IPV6",
    21075: "ELETROVIGILANCIA SERVICOS LTDA", 21076: "LOJAO DO CARLAO", 21077: "AGRO SAO LUIZ - IPV6",
    21078: "INOX BRIL", 21079: "ESQUADRIAS DE METAL L S LTDA", 21080: "LOJA VIPP - IPV6",
    21081: "CITA", 21082: "LOJA EMMY", 21083: "MASSAS NENA", 21084: "DISTRIBUIDORA OLHO D' AGUA",
    21085: "AUTO ELETRICA DE LIMA", 21086: "BICICLETARIA VITORIA", 21087: "RESTAURANTE E LANCHONETE SANTANNA",
    21088: "CESTAO DO CARLAO", 21089: "LOJA TOP - IPV4", 21090: "CAMPEIRA AGROVETERINARIA LTDA",
    21091: "FRAN PRESENTES", 21092: "LOJA EVE", 21093: "AGRO DUDU", 21094: "HOTEL SANTANA",
    21095: "TOKA SOM", 21096: "EMPRESA PENDENTE 21096", 21097: "SAWAYA", 21098: "METAL MINOZZO",
    21099: "X BURGUER", 21100: "LL PRESENTES", 21101: "LOJA DA MARIA", 21102: "OTICA HELENITA",
    21103: "ESPACO CASA DO SOL", 21104: "FALKS CONFECCOES", 21105: "CASA DO CEREAL",
    21106: "LAURA FLORES E PRESENTES", 21107: "PLANETA JEANS", 21108: "PLANETA CALÇADOS",
    21109: "POUSADA DONA MARIA", 21110: "CRUZVEL MOTOS", 21111: "SCHIEL",
    21112: "DSA ESQUADRIA E VIDRACARIA", 21113: "BETO", 21114: "LOJA DA NATALIA",
    21115: "MOTOS LEE", 21116: "DECORACOES ROSA", 21117: "CASA DE RACOES VIER UVA",
    21118: "HOTEL RIAD", 21119: "FABRICA DE TELAS CM", 21120: "CASA DE RACOES VIER PU",
    21121: "PLUS MATERIAIS ELETRICOS", 21122: "FMR", 21123: "DOELLE", 21124: "AK MATERIAIS",
    21125: "COMERCIAL CRJ", 21126: "WZ MECANICA", 21127: "BICHO MIMADO", 21128: "PORTELA",
    21129: "REAL PAPELARIA", 21130: "PREVI FIRE", 21131: "ENCANTO MODAS CM", 21132: "LOJA EVELYN",
    21133: "BOTECO DO SAFADAO", 21134: "DAISE MODAS FRONTIM", 21135: "IVONE MODAS",
    21136: "BABY STORE", 21137: "SALAO MIGUEL VARGAS", 21138: "RELOJOARIA CITIZEN"
}

PORTAS_CREDENCIAIS_ANTIGAS = {
    21001, 21002, 21003, 21004, 21005, 21006, 21007, 21008, 21009, 21010, 21012,
    21013, 21014, 21015, 21016, 21017, 21018, 21019, 21020, 21021, 21022, 21023,
    21024, 21025, 21026, 21027, 21028, 21029, 21030, 21031, 21032, 21033, 21034,
    21035, 21036, 21037, 21038, 21039, 21040, 21041, 21042, 21043, 21044, 21045,
    21046, 21047, 21048, 21049, 21050, 21051, 21052, 21053, 21054, 21055, 21056,
    21057, 21058, 21059, 21060, 21061, 21062, 21063, 21064, 21065, 21066, 21067,
    21068, 21069, 21070, 21071, 21072, 21073, 21074, 21075, 21076, 21077, 21078,
    21079, 21080, 21081, 21082, 21083, 21084, 21085, 21086, 21087, 21088, 21089,
    21090, 21091, 21092, 21093, 21094, 21095, 21096, 21097, 21098, 21099, 21100,
    21101, 21102, 21103, 21104, 21105, 21106, 21107, 21108, 21109, 21110, 21111,
    21112, 21113, 21114, 21115, 21116, 21117, 21118, 21119, 21120, 21121, 21122,
    21123, 21124, 21125, 21126, 21127, 21128, 21129, 21130, 21131, 21132, 21133,
    21134, 21135, 21136, 21137, 21138, 21011
}

def get_connection_details(porta):
    host = os.environ.get("DB_HOST")
    database = os.environ.get("DB_NAME")
    
    if porta in PORTAS_CREDENCIAIS_ANTIGAS:
        usuario = os.environ.get("DB_USER_OLD")
        senha = os.environ.get("DB_PASS_OLD")
    else:
        usuario = os.environ.get("DB_USER_NEW")
        senha = os.environ.get("DB_PASS_NEW")
        
    return {"host": host, "dbname": database, "user": usuario, "password": senha, "port": porta}

def verificar_por_nota(porta):
    conn_details = get_connection_details(porta)
    nome_empresa = EMPRESAS_POR_PORTA.get(porta, "N/A")
    hoje = datetime.now(timezone.utc).date()
    
    try:
        with psycopg2.connect(**conn_details, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(CAST(datano AS date)) FROM notas;")
                data_ultima = cur.fetchone()[0]

        if data_ultima is None:
            return {"porta": porta, "msg": f"[PORTA {porta}] {nome_empresa:<45} | ⚠️ AVISO: Tabela 'notas' vazia.", "tag": "aviso"}
        
        dias_sem_dados = (hoje - data_ultima).days
        if dias_sem_dados > 2:
            msg = f"[PORTA {porta}] {nome_empresa:<45} | ❌ ERRO: Última nota: {data_ultima.strftime('%d/%m/%Y')} ({dias_sem_dados} dias atrás)"
            tag = "erro"
        else:
            msg = f"[PORTA {porta}] {nome_empresa:<45} | ✅ OK - Última nota: {data_ultima.strftime('%d/%m/%Y')}"
            tag = "ok"
        return {"porta": porta, "msg": msg, "tag": tag}

    except psycopg2.OperationalError:
        return {"porta": porta, "msg": f"[PORTA {porta}] {nome_empresa:<45} | ❗ AVISO: Falha na conexão/autenticação.", "tag": "aviso"}
    except Exception:
        return {"porta": porta, "msg": f"[PORTA {porta}] {nome_empresa:<45} | ❌ ERRO: Falha ao consultar 'notas'.", "tag": "erro"}

def verificar_tamanho_banco(porta):
    conn_details = get_connection_details(porta)
    nome_empresa = EMPRESAS_POR_PORTA.get(porta, "N/A")
    
    query = """
        (SELECT
            datname AS banco,
            pg_database_size(datname) AS tamanho_bytes,
            pg_size_pretty(pg_database_size(datname)) AS tamanho_pretty
        FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres')
        ORDER BY pg_database_size(datname) DESC, banco ASC)
        UNION ALL
        (SELECT
            'TOTAL' AS banco,
            sum(pg_database_size(datname)) AS tamanho_bytes,
            pg_size_pretty(sum(pg_database_size(datname))) AS tamanho_pretty
        FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres'));
    """
    
    try:
        with psycopg2.connect(**conn_details, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
        
        linhas_formatadas = []
        total_size = 0
        for banco, tamanho_bytes, tamanho_pretty in results:
            if banco == 'TOTAL':
                linhas_formatadas.append({"msg": f"{'TOTAL':<25} | {tamanho_pretty:>15}", "tag": "total"})
                total_size = tamanho_bytes if tamanho_bytes is not None else 0
            else:
                linhas_formatadas.append({"msg": f"{banco:<25} | {tamanho_pretty:>15}", "tag": "db-name"})
        
        return {"porta": porta, "nome_empresa": nome_empresa, "linhas": linhas_formatadas, "total_size": total_size}

    except psycopg2.OperationalError:
        return {"porta": porta, "nome_empresa": nome_empresa, "linhas": [{"msg": f"❗ AVISO: Falha na conexão/autenticação.", "tag": "aviso"}], "total_size": -1}
    except Exception as e:
        return {"porta": porta, "nome_empresa": nome_empresa, "linhas": [{"msg": f"❌ ERRO: {str(e).strip()}", "tag": "erro"}], "total_size": -1}

@app.route('/api/check_replication', methods=['GET'])
def check_replication_handler():
    mode = request.args.get('mode', 'notes')
    sort_order = request.args.get('sort', 'port')
    
    portas_ordenadas = sorted(EMPRESAS_POR_PORTA.keys())
    
    if mode == 'notes':
        target_function = verificar_por_nota
        hoje = datetime.now(timezone.utc).date()
        header = f"🔍 Verificando por Última Nota... (Data de hoje: {hoje.strftime('%d/%m/%Y')})"
    elif mode == 'size':
        target_function = verificar_tamanho_banco
        header = f"📊 Verificando Tamanho dos Bancos de Dados..."
    else:
        return jsonify({"header": "Erro: Modo inválido", "results": []}), 400

    resultados_map = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_port = {executor.submit(target_function, porta): porta for porta in portas_ordenadas}
        
        for future in as_completed(future_to_port):
            porta = future_to_port[future]
            try:
                data = future.result()
                resultados_map[porta] = data
            except Exception as exc:
                nome_empresa = EMPRESAS_POR_PORTA.get(porta, "N/A")
                error_msg_dict = {"msg": f"[PORTA {porta}] {nome_empresa:<45} | ❌ ERRO FATAL NA THREAD: {exc}", "tag": "erro"}
                if mode == 'size':
                     resultados_map[porta] = {"porta": porta, "nome_empresa": nome_empresa, "linhas": [error_msg_dict], "total_size": -1}
                else: 
                     resultados_map[porta] = error_msg_dict

    # Ordenação dos resultados
    results = []
    if mode == 'size':
        # Transforma o mapa em uma lista de resultados para ordenar
        lista_de_resultados = list(resultados_map.values())
        
        if sort_order == 'size':
            # Ordena pela chave 'total_size', do maior para o menor
            lista_de_resultados.sort(key=lambda x: x.get('total_size', 0), reverse=True)
        else: # sort_order == 'port'
             lista_de_resultados.sort(key=lambda x: x.get('porta', 0))

        # Formata a saída após a ordenação
        for data in lista_de_resultados:
            if data:
                results.append({"msg": f"--- [PORTA {data['porta']}] {data['nome_empresa']} ---", "tag": "header"})
                results.extend(data['linhas'])
                results.append({"msg": "", "tag": ""})
    else:
        results = [resultados_map[porta] for porta in portas_ordenadas if porta in resultados_map]
            
    return jsonify({"header": header, "results": results})

