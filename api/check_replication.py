# -*- coding: utf-8 -*-
import os
import psycopg2
from flask import Flask, jsonify, request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# A Vercel exige que a aplicação Flask se chame 'app'
app = Flask(__name__)

# --- Configurações das VMs (Servidores) ---
# O host é o IP Fixo compartilhado para ambas as VMs, conforme solicitado.
FIXED_HOST = "186.211.103.3" 

# Nome da tabela de notas (confirmado: "notas")
NOTES_TABLE_NAME = "notas" 
# Nome do banco de dados da aplicação (corrigido para "ecf")
DEFAULT_DB_NAME = "ecf"

CONFIGURACOES_VM = {
    "221": {
        "HOST": FIXED_HOST,
        # Usando o default corrigido: 'ecf'
        "DB_NAME": os.environ.get("DB_NAME_221", DEFAULT_DB_NAME), 
        "USER_OLD": os.environ.get("DB_USER_OLD", "postgres_old"),
        "PASS_OLD": os.environ.get("DB_PASS_OLD", "senha_antiga"),
        "USER_NEW": os.environ.get("DB_USER_NEW", "postgres_new"),
        "PASS_NEW": os.environ.get("DB_PASS_NEW", "senha_nova"),
        "NOTES_TABLE": NOTES_TABLE_NAME # Tabela de notas
    },
    "222": {
        "HOST": FIXED_HOST,
        # Usando o default corrigido: 'ecf'
        "DB_NAME": os.environ.get("DB_NAME_222", DEFAULT_DB_NAME), 
        # Credenciais padrão para a VM 222 (replicador, la@246618)
        "USER_DEFAULT": "replicador",
        "PASS_DEFAULT": "la@246618", 
        "NOTES_TABLE": NOTES_TABLE_NAME # Tabela de notas
    }
}

# --- Dicionários de Configuração de Clientes ---
# Clientes da VM 221 (Antiga lógica)
EMPRESAS_VM_221 = {
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
    21029: "SERGIO ANDRUKIU UVA",
    21030: "XAVIER MARMORES",
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
    21095: "TOKA SOM", 21096: "LOJA CRIATIVA", 21097: "SAWAYA", 21098: "METAL MINOZZO",
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
    21136: "BABY STORE", 21137: "SALAO MIGUEL VARGAS", 21138: "RELOJOARIA CITIZEN",
    21139: "EUROFRIOS", 21140: "DISTRIBUIDORA BARAO", 21141: "MERCADO ROSANE", 21142: "DAISE MODAS MALLET",
    21143: "VINICOLA BORILLE", 21144: "LOJA DA TONIA", 21145: "CLUBE 7", 21146: "MERCADO DAMAR",
    21147: "RFL EQUIPAMENTOS", 21148: "SERRARIA WERLE", 21149: "VIVEIRO ANAFLORA", 21150: "PIONEIRA AGROVETERINARIA"
}

# Clientes da VM 222 (Novos clientes da imagem)
EMPRESAS_VM_222 = {
    22001: "PADARIA VOVO MILENE",
    22002: "LEAO GAS",
    22003: "MERCADO FAMILIAR",
    22004: "NEUTCHO MOTOS",
    22005: "DFATTO IND COM MOVEIS",
    22006: "FARMACIA CRISTO REI",
    22007: "HOTEL RODAK",
    22008: "AGRO MECANICA 047",
    22009: "CASA DAS ANTENAS",
    22010: "AGRO SANTO ANTONIO BITURUNA",
    22011: "MERCADO BOM DIA SERVIDOR 2",
    22012: "ADIR ZAREMBA - BOCÃO",
    22013: "AMILTON DZIRBA - BOCÃO",
    22014: "ANTONIO SAVICK - BOCÃO",
    22015: "CARVAO SÃO SILVESTRE - BOCÃO",
    22016: "CARVÃO TIGRE - BOCÃO",
    22017: "CARVOARIA RUBBO - BOCÃO",
    22018: "ERVA MATE SANTO ANTONIO - BOCÃO",
    22019: "CARROCERIAS DOIS IRMÃOS - BOCÃO",
    22020: "GILSON CAPELETE - BOCÃO",
    22021: "DISTRIBUIDORA LABELLY",
    22022: "MACIEL RUBBO - BOCÃO",
    22023: "SERRARIA USS - BOCÃO",
    22024: "RE EMPACOTADORA - BOCÃO",
    22025: "SABIA COMERCIO DE GAS E AGUA",
    22026: "SERRARIA NOVA CONCORDIA - BOCÃO",
    22027: "HERMES BAHNERT - BOCÃO",
    22028: "WIERZCHON - BOCÃO",
    22029: "AMELIA LISBOA - BOCÃO",
    22030: "PADARIA 3MAR - BOCÃO",
    22031: "DSA NFE - BOCÃO",
    22032: "MARCOS RUBBO - BOCÃO",
    22033: "MATZEMBACHER - BOCÃO",
    22034: "NOVA CONCORDIA FILIAL - BOCÃO",
    22035: "CARVÃO DA FAZENDA - BOCÃO",
    22036: "COMERCIAL SUL",
    22037: "REI DO JEANS",
    22038: "FLORICULTURA BELLA FLOR",
    22039: "RELOCENTER",
    22040: "TEIXEIRA AUTO PEÇAS",
    22041: "TRANSLAMINADOS",
    22042: "COMERCIAL BORSSATO",
    22043: "AÇOUGUE DO IVO",
    22044: "CIA DOS BICHOS",
    22045: "PRE MOLDADOS SC",
    22046: "RESTAURANTE JANGADA",
    22047: "PONTO DA MODA",
    22048: "MERCADO ADRIANE",
    22049: "MM CELL CRUZ MACHADO",
    22050: "GR EMBALAGENS",
    22051: "SUDELAR MOVEIS",
    22052: "LABEL COSMETICOS"
}


# Portas que usam credenciais antigas na VM 221
PORTAS_CREDENCIAIS_ANTIGAS_221 = {
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
    21134, 21135, 21136, 21137, 21138, 21011, 21139, 21140, 21141, 21142, 21143,
    21144, 21145, 21146, 21147, 21148, 21149, 21150
}

def get_connection_details(id_vm, porta):
    """
    Retorna os detalhes de conexão para uma porta e ID de VM específicos.
    """
    vm_config = CONFIGURACOES_VM.get(id_vm)
    if not vm_config:
        # Isso não deve acontecer se o frontend funcionar corretamente, mas é uma proteção
        raise ValueError(f"Configuração para VM {id_vm} não encontrada.")
    
    host = vm_config["HOST"]
    database = vm_config["DB_NAME"]
    
    if id_vm == "221":
        # Lógica de credenciais para VM 221
        if porta in PORTAS_CREDENCIAIS_ANTIGAS_221:
            usuario = vm_config["USER_OLD"]
            senha = vm_config["PASS_OLD"]
        else:
            usuario = vm_config["USER_NEW"]
            senha = vm_config["PASS_NEW"]
    
    elif id_vm == "222":
        # Lógica de credenciais para VM 222 (usa credenciais padrão fornecidas)
        usuario = vm_config["USER_DEFAULT"]
        senha = vm_config["PASS_DEFAULT"]
    
    else:
        raise ValueError(f"ID de VM {id_vm} inválido.")
        
    # Retorna o dicionário de detalhes, incluindo o host corrigido
    return {"host": host, "dbname": database, "user": usuario, "password": senha, "port": porta}

def get_empresas_by_vm(id_vm):
    """
    Retorna o dicionário de empresas para o ID de VM fornecido.
    """
    if id_vm == "221":
        return EMPRESAS_VM_221
    elif id_vm == "222":
        return EMPRESAS_VM_222
    return {}

def verificar_por_nota(id_vm, porta):
    """
    Verifica a data da última nota para a porta e VM especificadas.
    """
    try:
        conn_details = get_connection_details(id_vm, porta)
    except ValueError as e:
        return {"porta": porta, "msg": f"[VM {id_vm}] N/A | ❌ ERRO: {str(e)}", "tag": "erro"}

    empresas = get_empresas_by_vm(id_vm)
    nome_empresa = empresas.get(porta, "N/A")
    hoje = datetime.now(timezone.utc).date()
    
    # Obtém o nome da tabela configurado para a VM
    notes_table = CONFIGURACOES_VM.get(id_vm, {}).get("NOTES_TABLE", NOTES_TABLE_NAME)
    
    try:
        # Tenta a conexão com um timeout de 5 segundos
        with psycopg2.connect(**conn_details, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                # Consulta para pegar a data máxima da nota, usando o nome da tabela configurado
                query = f"SELECT MAX(CAST(datano AS date)) FROM {notes_table};"
                cur.execute(query)
                data_ultima = cur.fetchone()[0]

        if data_ultima is None:
            return {"porta": porta, "msg": f"[VM {id_vm}] {nome_empresa:<45} | ⚠️ AVISO: Tabela '{notes_table}' vazia.", "tag": "aviso"}
        
        dias_sem_dados = (hoje - data_ultima).days
        # Alerta se a nota for de 3 ou mais dias atrás
        if dias_sem_dados >= 3:
            msg = f"[VM {id_vm}] {nome_empresa:<45} | ❌ ERRO: Última nota: {data_ultima.strftime('%d/%m/%Y')} ({dias_sem_dados} dias atrás)"
            tag = "erro"
        elif dias_sem_dados == 2:
            msg = f"[VM {id_vm}] {nome_empresa:<45} | ⚠️ AVISO: Última nota: {data_ultima.strftime('%d/%m/%Y')} ({dias_sem_dados} dias atrás)"
            tag = "aviso"
        else:
            msg = f"[VM {id_vm}] {nome_empresa:<45} | ✅ OK - Última nota: {data_ultima.strftime('%d/%m/%Y')}"
            tag = "ok"
        return {"porta": porta, "msg": msg, "tag": tag}

    except psycopg2.OperationalError:
        return {"porta": porta, "msg": f"[VM {id_vm}] {nome_empresa:<45} | ❗ CONEXÃO: Falha ao conectar/autenticar. (Host: {conn_details.get('host', 'N/A')}, DB: {conn_details.get('dbname', 'N/A')})", "tag": "aviso"}
    except Exception as e:
        # Retorna o erro exato, incluindo o problema da tabela
        error_msg = str(e).strip().replace('\n', ' | ')
        return {"porta": porta, "msg": f"[VM {id_vm}] {nome_empresa:<45} | ❌ ERRO GERAL: Falha ao consultar '{notes_table}'. ({error_msg})", "tag": "erro"}

def verificar_tamanho_banco(id_vm, porta):
    """
    Verifica o tamanho do banco de dados para a porta e VM especificadas.
    """
    try:
        conn_details = get_connection_details(id_vm, porta)
    except ValueError as e:
        return {"porta": porta, "nome_empresa": "N/A", "linhas": [{"msg": f"❌ ERRO: {str(e)}", "tag": "erro"}], "total_size": -1}

    empresas = get_empresas_by_vm(id_vm)
    nome_empresa = empresas.get(porta, "N/A")
    
    # Consulta SQL para obter o tamanho de cada banco e o total
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

    except psycopg2.OperationalError as e:
        return {"porta": porta, "nome_empresa": nome_empresa, "linhas": [{"msg": f"❗ CONEXÃO: Falha ao conectar/autenticar. (Host: {conn_details.get('host', 'N/A')}, DB: {conn_details.get('dbname', 'N/A')})", "tag": "aviso"}], "total_size": -1}
    except Exception as e:
        return {"porta": porta, "nome_empresa": nome_empresa, "linhas": [{"msg": f"❌ ERRO GERAL: {str(e).strip()}", "tag": "erro"}], "total_size": -1}

@app.route('/api/check_replication', methods=['GET'])
def check_replication_handler():
    # Parâmetro para selecionar a VM (221 ou 222). Padrão é '221'.
    vm_id = request.args.get('vm_id', '221')
    mode = request.args.get('mode', 'notes')
    sort_order = request.args.get('sort', 'port')
    
    empresas = get_empresas_by_vm(vm_id)
    if not empresas:
        return jsonify({"header": f"Erro: VM {vm_id} não configurada.", "results": []}), 400

    portas_ordenadas = sorted(empresas.keys())
    
    if mode == 'notes':
        target_function = lambda porta: verificar_por_nota(vm_id, porta)
        hoje = datetime.now(timezone.utc).date()
        header = f"🔍 VM {vm_id} | Verificando por Última Nota... (Hoje: {hoje.strftime('%d/%m/%Y')})"
    elif mode == 'size':
        target_function = lambda porta: verificar_tamanho_banco(vm_id, porta)
        header = f"📊 VM {vm_id} | Verificando Tamanho dos Bancos de Dados..."
    else:
        return jsonify({"header": "Erro: Modo inválido", "results": []}), 400

    resultados_map = {}
    with ThreadPoolExecutor(max_workers=30) as executor: 
        # Executa a função 'target_function' que já tem o 'vm_id' embutido
        future_to_port = {executor.submit(target_function, porta): porta for porta in portas_ordenadas}
        
        for future in as_completed(future_to_port):
            porta = future_to_port[future]
            try:
                data = future.result()
                resultados_map[porta] = data
            except Exception as exc:
                nome_empresa = empresas.get(porta, "N/A")
                error_msg_dict = {"msg": f"[VM {vm_id}] {nome_empresa:<45} | ❌ ERRO FATAL NA THREAD: {exc}", "tag": "erro"}
                if mode == 'size':
                     resultados_map[porta] = {"porta": porta, "nome_empresa": nome_empresa, "linhas": [error_msg_dict], "total_size": -1}
                else: 
                     resultados_map[porta] = error_msg_dict

    # Ordenação dos resultados
    results = []
    if mode == 'size':
        lista_de_resultados = list(resultados_map.values())
        
        if sort_order == 'size':
            # Ordena pela chave 'total_size', do maior para o menor
            lista_de_resultados.sort(key=lambda x: x.get('total_size', 0), reverse=True)
        else: # sort_order == 'port'
             lista_de_resultados.sort(key=lambda x: x.get('porta', 0))

        # Formata a saída após a ordenação
        for data in lista_de_resultados:
            if data:
                # Usa o nome da empresa como cabeçalho para o modo de tamanho
                results.append({"msg": f"--- [VM {vm_id} - PORTA {data['porta']}] {data['nome_empresa']} ---", "tag": "header"})
                results.extend(data['linhas'])
                results.append({"msg": "", "tag": ""})
    else:
        # Modo 'notes' - A ordenação é sempre por porta (que já é garantida pelo loop de portas_ordenadas)
        results = [resultados_map[porta] for porta in portas_ordenadas if porta in resultados_map]
            
    return jsonify({"header": header, "results": results})
