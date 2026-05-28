"""
Monitor de Notícias v1.0 - Análise de cobertura jornalística brasileira
=======================================================================
Fontes: G1, Folha, O Globo, Estadão, Metrópoles, Intercept, Agência Mural,
        A Pública, Fiocruz, Jornal USP, Agência Galão

ARQUITETURA:
  1. Coleta RSS / HTML de cada fonte (Playwright para JS-heavy)
  2. Clusteriza manchetes por tokens significativos compartilhados
  3. Analisa cobertura: trending (≥3 fontes) vs exclusivo (1 fonte)
  4. Extrai 5W por cluster (Quem, O quê, Quando, Onde, Por quê)
  5. Gera sugestões de pauta baseadas em gaps e desdobramentos
  6. Envia relatório estruturado ao Telegram

Secrets: TELEGRAM_TOKEN, CHAT_ID
Schedule: diário 7h BRT (10:00 UTC)
"""

import requests, datetime, os, sys, re, json, unicodedata, time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
if not TELEGRAM_TOKEN or not CHAT_ID:
    print("FATAL: TELEGRAM_TOKEN ou CHAT_ID ausentes."); sys.exit(1)

# ── FONTES ────────────────────────────────────────────────────────
SOURCES = [
    # Grande imprensa
    {"name": "G1",           "emoji": "🔵", "tier": "grande",
     "rss":  "https://g1.globo.com/dynamo/ultimas-noticias/rss2.xml",
     "home": "https://g1.globo.com/",
     "fmt":  "globo_json"},   # Globo uses JSON, not XML
    {"name": "G1-SP",        "emoji": "🔵", "tier": "grande",
     "rss":  "https://g1.globo.com/dynamo/sao-paulo/rss2.xml",
     "home": "https://g1.globo.com/sp/sao-paulo/",
     "fmt":  "globo_json"},
    {"name": "Folha",        "emoji": "🟠", "tier": "grande",
     "rss":  "https://feeds.folha.uol.com.br/emcimadahora/rss091.xml",
     "home": "https://www1.folha.uol.com.br/ultimas-noticias/",
     "fmt":  "rss"},
    {"name": "Estadão",      "emoji": "🔴", "tier": "grande",
     "rss":  "https://www.estadao.com.br/arc/outboundfeeds/rss/?outputType=xml",
     "rss2": "https://www.estadao.com.br/ultimas/",
     "home": "https://www.estadao.com.br/ultimas/",
     "fmt":  "rss"},
    {"name": "O Globo",      "emoji": "⚫", "tier": "grande",
     "rss":  "https://oglobo.globo.com/arc/outboundfeeds/rss/?outputType=xml",
     "rss2": "https://oglobo.globo.com/ultimas-noticias/",
     "home": "https://oglobo.globo.com/ultimas-noticias/",
     "fmt":  "rss"},
    {"name": "Metrópoles",   "emoji": "🟣", "tier": "grande",
     "rss":  "https://www.metropoles.com/feed/",
     "home": "https://www.metropoles.com/",
     "fmt":  "rss"},
    # Investigativa / especializada
    {"name": "Intercept",    "emoji": "🔷", "tier": "investigativa",
     "rss":  "https://www.intercept.com.br/feed/",
     "home": "https://www.intercept.com.br/",
     "fmt":  "rss"},
    {"name": "A Pública",    "emoji": "🟢", "tier": "investigativa",
     "rss":  "https://apublica.org/feed/",
     "home": "https://apublica.org/",
     "fmt":  "rss"},
    {"name": "Ag. Mural",    "emoji": "🟡", "tier": "investigativa",
     "rss":  "https://agenciamural.org.br/wp-json/wp/v2/posts?per_page=25&orderby=date&order=desc&_fields=title,link,excerpt,date",
     "rss2": "https://agenciamural.org.br/feed/",
     "rss3": "https://agenciamural.org.br/noticias/",
     "home": "https://agenciamural.org.br/noticias/",
     "fmt":  "wp_api"},
    # Científica / acadêmica
    {"name": "Fiocruz",      "emoji": "🏥", "tier": "cientifica",
     "rss":  "https://agencia.fiocruz.br/feed/",
     "rss2": "https://agencia.fiocruz.br/wp-json/wp/v2/posts?per_page=25&orderby=date&order=desc&_fields=title,link,excerpt,date",
     "rss3": "https://agencia.fiocruz.br/noticias",
     "home": "https://agencia.fiocruz.br/",
     "fmt":  "rss"},
    {"name": "Jornal USP",   "emoji": "🎓", "tier": "cientifica",
     "rss":  "https://jornal.usp.br/feed/",
     "home": "https://jornal.usp.br/",
     "fmt":  "rss"},
    {"name": "Ag. Galão",    "emoji": "🎭", "tier": "cultural",
     "rss":  "https://agenciagalo.com/wp-json/wp/v2/posts?per_page=25&orderby=date&order=desc&_fields=title,link,excerpt,date",
     "rss2": "https://agenciagalo.com/feed/",
     "rss3": "https://agenciagalo.com/",
     "home": "https://agenciagalo.com/",
     "fmt":  "wp_api"},
]


# ── STOPWORDS E NORMALIZAÇÃO ─────────────────────────────────────
STOPWORDS = {
    "de","da","do","das","dos","em","na","no","nas","nos","a","o","as","os",
    "e","é","ao","aos","às","um","uma","uns","umas","para","por","com","que",
    "se","ou","mas","mais","já","ainda","após","sobre","entre","durante","após",
    "pelo","pela","pelos","pelas","até","sem","contra","como","quando","onde",
    "ser","estar","ter","foi","são","foram","está","estão","vai","vão","tem",
    "têm","teve","houve","há","diz","deu","vai","pode","deve","novo","nova",
    "dois","três","mil","neste","nesta","nessa","nesse","esse","esta","isto",
    "isso","este","aquele","aquela","diante","frente","agora","hoje","ontem",
}
# ── TOPIC RELEVANCE FILTER ────────────────────────────────────────
# KEEP: politics, economy, SP city, state/federal government, science, environment, health
_KEEP_TOKENS = {
    # Política
    "politica","eleicao","candidato","partido","voto","congresso","senado","camara",
    "presidente","governador","prefeito","ministro","ministerio","governo","reforma",
    "lei","decreto","votacao","aprovado","aprovada","deputado","vereador","senador",
    # Economia
    "economia","economico","mercado","dolar","real","inflacao","pib","desemprego",
    "emprego","empresa","industria","exportacao","banco","bolsa","fiscal","orcamento",
    "investimento","juros","selic","tributario","imposto","arrecadacao","privatizacao",
    "concessao","licitacao","contrato","corrupcao","desvio","fraude",
    # São Paulo (cidade e estado)
    "paulo","paulista","paulistano","tarcisio","alesp","estadual","capital","subprefeitura",
    # Governo federal
    "lula","planalto","stf","supremo","tcm","tcu","tce","ministerio","federal",
    # Ciência e pesquisa
    "pesquisa","ciencia","cientifico","estudo","universidade","descoberta","tecnologia",
    "inovacao","academico","academica","publicado","revista","revista","cientifico",
    # Saúde
    "saude","doenca","virus","vacina","hospital","sus","medico","pandemia","epidemia",
    "dengue","cancer","tratamento","medicamento","anvisa","fiocruz","clinica","cirurgia",
    # Meio ambiente
    "ambiental","desmatamento","clima","climatica","aquecimento","biodiversidade",
    "sustentabilidade","floresta","queimada","carbono","emissao","enchente","seca",
    "poluicao","reciclagem","energia","renovavel","solar","eolica","hidroeletrica",
    # Segurança pública
    "policia","crime","violencia","morte","assassinato","homicidio","operacao",
    "prisao","condenado","investigacao","corrupcao","trafico","arma",
}

# OUT: celebrities, gossip, food, entertainment, lifestyle
_BLOCK_TOKENS = {
    "celebridade","famoso","famosa","ator","atriz","cantor","cantora","artista",
    "fofoca","novela","reality","bbb","bigbrother","namorado","namorada","casamento",
    "separacao","divorcio","gravidez",
    "receita","culinaria","gastronomia","chef","restaurante","prato","ingrediente",
    "moda","roupa","look","estilo","beleza","maquiagem","skincare","cabelo",
    "horoscopo","signo",
    "serie","filme","cinema","streaming","netflix","disney","amazon",
    "viagem","turismo","praia","hotel","hospedagem","passeio",
    "ingresso","turnê","show","concerto",  # entertainment events
}

# Hard-block phrases that always mean celebrity content
_BLOCK_HARD_PHRASES = [
    "anuncia show", "anuncia turnê", "anuncia tour", "venda de ingressos",
    "onde comprar ingresso", "data e local", "saiba data", "como comprar",
    "receita de", "como fazer", "dicas para", "veja como",
    "look do dia", "estilo de", "antes e depois",
    "o que comer", "onde comer", "melhor restaurante",
]

# Topic-specific block phrases (must match as substring in normalized title)
_BLOCK_PHRASES = [
    "receita de", "como fazer", "dicas para", "o melhor de",
    "o que comer", "onde comer", "melhor restaurante",
    "look do dia", "tendencia de moda", "cabelo",
    "top 10", "lista de", "ranking dos mais",
]


# Tokens that establish a Brazilian angle for international stories
_BRAZIL_TOKENS = {
    # País e gentílicos
    "brasil","brasileiro","brasileira","brasileiros","brasileiras",
    # Cidades e estados-chave
    "paulo","paulista","carioca","brasilia","mineiro","gaucho",
    "baiano","cearense","pernambucano","fluminense","minas",
    # Políticos
    "lula","bolsonaro","tarcisio","haddad","moro","flavio","eduardo",
    "gleisi","tebet","pacheco","lira","ciro","marina","damares",
    # Instituições
    "stf","stj","tcu","tse","pgr","mpf","ibge","ibama","funai","inpe",
    "camara","senado","congresso","planalto","governo",
    # Moeda e economia BR
    "real","reais","brl","selic","ipca","ibovespa",
    # Partidos
    "pt","pl","mdb","psdb","pdT","psd","republicanos","solidariedade",
    # Violência no BR (requer contexto)
    "feminicidio","femicidio","mulher",
}

def is_relevant(article):
    """Return True if the article matches monitored topics and isn't in blocked categories."""
    title_low = normalize(article.title)
    desc_low  = normalize(article.description)
    combined  = title_low + " " + desc_low

    tokens = set(re.findall(r"[a-zà-ÿ]{4,}", combined))

    # Hard-block phrases (always reject regardless of other signals)
    for phrase in _BLOCK_HARD_PHRASES:
        if normalize(phrase) in combined:
            return False
    # Legacy block phrases
    for phrase in _BLOCK_PHRASES:
        if normalize(phrase) in combined:
            return False

    # If mostly blocked tokens → skip
    blocked_hits = len(tokens & _BLOCK_TOKENS)
    keep_hits    = len(tokens & _KEEP_TOKENS)

    if blocked_hits >= 2 and keep_hits == 0:
        return False

    # International stories must have a Brazilian angle
    has_brazil = bool(tokens & _BRAZIL_TOKENS)

    # If nothing relevant AND no Brazil angle AND not investigative source → skip
    if keep_hits == 0 and not has_brazil and article.source not in {"Intercept","A Pública","Ag. Mural","Fiocruz"}:
        return False

    # If marginally relevant but zero Brazil angle → skip (e.g. German crime story)
    if keep_hits <= 1 and not has_brazil:
        return False

    return True


# ── 5-WORD SYNTHESIS ─────────────────────────────────────────────
def short_headline(title, description=""):
    """
    Produce a compact readable headline for the summary message.
    Uses the actual cleaned title, not token extraction.
    Steps: clean dateline → strip EXCLUSIVO → trim at punctuation → 55 chars.
    """
    t = clean_headline(title).strip()
    # Remove trailing boilerplate after "; saiba", "; veja", "; entenda"
    t = re.sub(r"\s*[;:,]\s*(?:saiba|veja|entenda|confira|leia|entenda\s+mais).*$", "", t, flags=re.I)
    # Trim to first natural break if too long
    if len(t) > 55:
        for sep in [";", " — ", " - ", ","]:
            idx = t.find(sep)
            if 25 < idx < 60:
                t = t[:idx].strip()
                break
    # Hard cap
    if len(t) > 60: t = t[:57] + "…"
    return t



# Wire datelines# Wire datelines that contaminate headlines
_DATELINES = re.compile(
    r"^(?:BRASÍLIA|SÃO PAULO|RIO DE JANEIRO|SÃO PAULO|BELO HORIZONTE|"
    r"SALVADOR|FORTALEZA|RECIFE|MANAUS|CURITIBA|PORTO ALEGRE|BELÉM|"
    r"GOIÂNIA|FLORIANÓPOLIS|CAMPO GRANDE|TERESINA|MACEIÓ|NATAL|JOÃO PESSOA|"
    r"ARACAJU|MACAPÁ|BOA VISTA|PORTO VELHO|RIO BRANCO|PALMAS|VITÓRIA|"
    r"BRASILIA|SAO PAULO|RIO)\s*[-–]\s*",
    re.I | re.U
)
_EXCLUSIVO = re.compile(r"^(?:EXCLUSIVO|EXCLUSIVA|ESPECIAL|BREAKING|ALERTA|ATENÇÃO)\s*[:\-]?\s*", re.I)

def clean_headline(title):
    """Remove wire datelines and EXCLUSIVO prefixes before synthesis."""
    t = _DATELINES.sub("", title).strip()
    t = _EXCLUSIVO.sub("", t).strip()
    return t

def normalize(t):
    return "".join(c for c in unicodedata.normalize("NFKD", t.lower())
                   if not unicodedata.combining(c))

def tokenize(text):
    tokens = re.findall(r"[a-z\u00e0-\u00ff]{4,}", normalize(text))
    return {t for t in tokens if t not in STOPWORDS}

def clean_html(text):
    if not text: return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()

# ── DATA MODEL ───────────────────────────────────────────────────
class Article:
    def __init__(self, title, link, description, pub_date, source_name, source_emoji):
        self.title       = clean_headline(title.strip())
        self.link        = link.strip() if link else ""
        _d = clean_html(description or "")
        if len(_d) > 400:
            cut = _d.rfind('. ', 0, 400)
            _d = _d[:cut+1] if cut > 100 else _d[:400]
        self.description = _d
        self.pub_date    = pub_date or ""
        self.source      = source_name
        self.emoji       = source_emoji
        self.tokens      = tokenize(title)

    def age_hours(self):
        """How many hours ago was this published."""
        for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                    "%Y-%m-%dT%H:%M:%S%z"]:
            try:
                import email.utils
                dt = email.utils.parsedate_to_datetime(self.pub_date)
                now = datetime.datetime.now(datetime.timezone.utc)
                return (now - dt).total_seconds() / 3600
            except: pass
        return 99.0  # unknown age

    def is_recent(self, hours=24):
        return self.age_hours() <= hours

# ── RSS FETCHER ───────────────────────────────────────────────────
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept": "application/rss+xml,application/xml,text/html,*/*",
           "Accept-Language": "pt-BR,pt;q=0.9"}


def fetch_rss(source):
    """
    Universal feed fetcher. Handles:
      - Standard RSS 2.0 (item elements)
      - Atom feeds (entry elements)
      - Globo JSON dynamo format
      - JSON Feed (jsonfeed.org spec)
      - HTML fallback via Playwright
    """
    articles = []
    name  = source["name"]
    emoji = source["emoji"]
    urls  = [source.get("rss",""), source.get("rss2",""), source.get("rss3","")]
    urls  = [u for u in urls if u]

    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                arts = _parse_response(r, source)
                if arts:
                    print(f"  {name}: {len(arts)} artigos ✅  [{url[-45:]}]")
                    return arts
                else:
                    snippet = ' '.join(r.text[:120].split())
                    print(f"  {name}: 200 / 0 itens - {snippet[:80]}")
            elif r.status_code >= 500:
                # Transient server error - retry once after 3s
                print(f"  {name}: HTTP {r.status_code} (retry in 3s)...")
                time.sleep(3)
                try:
                    r2 = requests.get(url, headers=HEADERS, timeout=15)
                    if r2.status_code == 200:
                        arts = _parse_response(r2, source)
                        if arts:
                            print(f"  {name}: {len(arts)} artigos ✅ (retry)")
                            return arts
                except Exception: pass
                print(f"  {name}: HTTP {r.status_code} após retry")
            else:
                print(f"  {name}: HTTP {r.status_code}  [{url[-45:]}]")
        except Exception as e:
            print(f"  {name}: {e.__class__.__name__}: {str(e)[:60]}")

    # Mark as needing Playwright - actual scraping done in main() with shared browser
    print(f"  {name}: ❌ sem artigos (marcado para Playwright)")
    source["_needs_playwright"] = True
    return []


def _parse_response(r, source):
    """Parse HTTP response - detect format and extract articles."""
    name  = source["name"]
    emoji = source["emoji"]
    fmt   = source.get("fmt","rss")
    ct    = r.headers.get("content-type","").lower()
    body  = r.content
    text  = r.text

    head = text.lstrip()[:10]

    # ── WordPress REST API ───────────────────────────────────
    if fmt == "wp_api" or "/wp-json/" in (source.get("rss","") + source.get("rss2","")):
        if head.startswith("[") or "json" in ct:
            arts = _parse_wp_api(text, name, emoji)
            if arts: return arts

    # ── XML / RSS / Atom  (check BEFORE JSON — G1 sends XML not JSON) ──
    is_xml = ("xml" in ct or "rss" in ct or
              head.startswith("<?xml") or head.startswith("<rss") or
              head.startswith("<feed") or head.startswith("<RSS"))
    if is_xml:
        arts = _parse_xml_feed(body, name, emoji)
        if arts: return arts

    # ── JSON / Globo Dynamo ──────────────────────────────────
    is_json = "json" in ct or head.startswith("{") or head.startswith("[")
    if is_json or fmt == "globo_json":
        arts = _parse_json_feed(text, name, emoji)
        if arts: return arts

    # ── HTML fallback ────────────────────────────────────────
    if "html" in ct or "<html" in text.lower()[:200]:
        return _parse_html_feed(text, name, emoji)

    return []



def _parse_wp_api(text, name, emoji):
    """Parse WordPress REST API JSON (GET /wp-json/wp/v2/posts)."""
    articles = []
    try:
        posts = json.loads(text)
        if not isinstance(posts, list):
            return []
        for post in posts:
            tr = post.get("title", {})
            title = clean_html(tr.get("rendered","") if isinstance(tr,dict) else str(tr))
            link  = post.get("link", "")
            er    = post.get("excerpt", {})
            desc  = clean_html(er.get("rendered","") if isinstance(er,dict) else str(er))
            date  = post.get("date", "")
            if title and len(title) > 10:
                articles.append(Article(title, link, desc, date, name, emoji))
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        pass
    return articles

def _parse_xml_feed(body, name, emoji):
    """Parse RSS 2.0 and Atom XML feeds. Handles namespaces."""
    articles = []
    try:
        root = ET.fromstring(body)
        tag = root.tag.lower()

        # RSS 2.0
        if "rss" in tag:
            for item in root.findall(".//item"):
                def g(t): el=item.find(t); return (el.text or "").strip() if el is not None else ""
                title = g("title")
                link  = g("link") or g("guid")
                desc  = g("description") or g("{http://purl.org/rss/1.0/modules/content/}encoded")
                date  = g("pubDate")
                if title and len(title) > 10:
                    articles.append(Article(clean_html(title), link, desc, date, name, emoji))

        # Atom
        elif "feed" in tag or "{http://www.w3.org/2005/Atom}" in root.tag:
            ns = "http://www.w3.org/2005/Atom"
            for entry in root.findall(f"{{{ns}}}entry"):
                def ga(t):
                    el = entry.find(f"{{{ns}}}{t}")
                    return (el.text or "").strip() if el is not None else ""
                title = ga("title")
                link  = ""
                lel   = entry.find(f"{{{ns}}}link")
                if lel is not None: link = lel.get("href","")
                desc  = ga("summary") or ga("content")
                date  = ga("published") or ga("updated")
                if title and len(title) > 10:
                    articles.append(Article(clean_html(title), link, desc, date, name, emoji))

        # RSS 1.0 / RDF
        else:
            ns_rss = "http://purl.org/rss/1.0/"
            for item in root.findall(f"{{{ns_rss}}}item"):
                title = item.findtext(f"{{{ns_rss}}}title") or ""
                link  = item.findtext(f"{{{ns_rss}}}link") or ""
                desc  = item.findtext(f"{{{ns_rss}}}description") or ""
                if title and len(title) > 10:
                    articles.append(Article(clean_html(title), link, desc, "", name, emoji))

    except ET.ParseError:
        pass
    return articles


def _parse_json_feed(text, name, emoji):
    """Parse JSON Feed spec, Globo Dynamo format, or any reasonable JSON."""
    articles = []
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return []

    # JSON Feed 1.x spec
    if isinstance(data, dict) and "items" in data:
        items = data["items"]
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    title = (item.get("title") or item.get("headline") or
                             item.get("summary",""))
                    link  = (item.get("url") or item.get("external_url") or
                             item.get("id",""))
                    desc  = (item.get("content_text") or item.get("content_html") or
                             item.get("summary",""))
                    date  = item.get("date_published") or item.get("date_modified","")
                    # Globo Dynamo nested format
                    if not title and "content" in item and isinstance(item["content"], dict):
                        c = item["content"]
                        title = c.get("title") or c.get("headline","")
                        link  = c.get("url","")
                        desc  = c.get("summary","")
                        date  = c.get("created","")
                    if title and len(clean_html(title)) > 10:
                        articles.append(Article(
                            clean_html(title), link, clean_html(desc), date, name, emoji))

    # Array at root
    elif isinstance(data, list):
        for item in data[:50]:
            if isinstance(item, dict):
                title = item.get("title") or item.get("headline","")
                link  = item.get("url") or item.get("link","")
                desc  = item.get("description") or item.get("summary","")
                if title and len(title) > 10:
                    articles.append(Article(clean_html(title), link, desc, "", name, emoji))

    return articles


def _parse_html_feed(text, name, emoji):
    """
    Extract headlines from HTML using JSON-LD, og:tags, and heading patterns.
    Used when a site serves HTML instead of a feed.
    """
    articles = []

    # JSON-LD structured data (NewsArticle schema)
    for blob in re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                           text, re.DOTALL):
        try:
            data = json.loads(blob)
            if not isinstance(data, list): data = [data]
            for obj in data:
                if obj.get("@type") in ("NewsArticle","Article","WebPage"):
                    title = obj.get("headline","")
                    link  = obj.get("url","")
                    desc  = obj.get("description","")
                    date  = obj.get("datePublished","")
                    if title and len(title) > 15:
                        articles.append(Article(title, link, desc, date, name, emoji))
        except: pass

    if articles: return articles[:30]

    # Open Graph tags
    og_title = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]{20,120})"', text)
    og_url   = re.search(r'<meta[^>]+property="og:url"[^>]+content="([^"]+)"', text)
    og_desc  = re.search(r'<meta[^>]+property="og:description"[^>]+content="([^"]+)"', text)
    if og_title:
        articles.append(Article(
            og_title.group(1),
            og_url.group(1) if og_url else "",
            og_desc.group(1) if og_desc else "",
            "", name, emoji))

    # Link + heading pattern
    seen = set()
    for href, txt in re.findall(r'<a[^>]+href="(https?://[^"]+)"[^>]*>([^<]{25,120})</a>', text):
        t = clean_html(txt).strip()
        if len(t) > 25 and t not in seen and not any(
                x in t.lower() for x in ["menu","login","assine","cadastre","newsletter"]):
            seen.add(t)
            articles.append(Article(t, href, "", "", name, emoji))
            if len(articles) >= 30: break

    return articles


def _playwright_scrape_batch(sources):
    """Run Playwright once, scrape all given sources with shared browser."""
    results = {}
    try:
        from playwright.sync_api import sync_playwright
        from urllib.parse import urljoin
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"])
            for source in sources:
                name  = source["name"]
                emoji = source["emoji"]
                url   = source.get("home", source.get("rss",""))
                if not url:
                    results[name] = []; continue
                articles = []
                try:
                    ctx  = browser.new_context(user_agent=UA, locale="pt-BR")
                    page = ctx.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    page.wait_for_timeout(2000)
                    for loc in page.query_selector_all("a h1, a h2, a h3, article h2, .card h2, h2 a, h3 a"):
                        try:
                            text = (loc.inner_text() or "").strip()
                            if len(text) < 20: continue
                            parent = loc.evaluate_handle("el => el.closest('a') || el.querySelector('a')")
                            href   = parent.get_attribute("href") if parent else ""
                            if href and not href.startswith("http"):
                                href = urljoin(url, href)
                            articles.append(Article(text, href or "", "", "", name, emoji))
                            if len(articles) >= 25: break
                        except: continue
                    ctx.close()
                except Exception as e:
                    print(f"  {name} PW err: {e}")
                results[name] = articles
            browser.close()
    except ImportError:
        for s in sources: results[s["name"]] = []
    except Exception as e:
        print(f"  PW batch err: {e}")
        for s in sources: results.setdefault(s["name"], [])
    return results


def _playwright_scrape(source):
    """Legacy single-source Playwright fallback (used if batch not available)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []

    name  = source["name"]
    emoji = source["emoji"]
    url   = source.get("home", source.get("rss",""))
    if not url: return []

    articles = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"])
            ctx  = browser.new_context(user_agent=UA, locale="pt-BR")
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(3000)

            # Try to find headlines: <h1/h2/h3> inside <a> tags
            for loc in page.query_selector_all("a h1, a h2, a h3, article h2, .card h2"):
                try:
                    text = (loc.inner_text() or "").strip()
                    if len(text) < 20: continue
                    parent = loc.evaluate_handle("el => el.closest('a')")
                    href   = parent.get_attribute("href") if parent else ""
                    if href and not href.startswith("http"):
                        from urllib.parse import urljoin
                        href = urljoin(url, href)
                    articles.append(Article(text, href or "", "", "", name, emoji))
                    if len(articles) >= 25: break
                except: continue

            browser.close()
    except Exception as e:
        print(f"  {name} PW: {e}")
    return articles



# ── CLUSTERING ───────────────────────────────────────────────────
def cluster_articles(articles, min_shared=2, window_hours=24):
    """
    Group articles covering the same story.
    Two articles are in the same cluster if they share >= min_shared significant tokens.
    Returns list of clusters sorted by number of sources.
    """
    total_recent = sum(1 for a in articles if a.is_recent(window_hours))
    recent = [a for a in articles if a.is_recent(window_hours) and is_relevant(a)]
    print(f"  Artigos: {len(articles)} total → {total_recent} recentes ({window_hours}h) → {len(recent)} relevantes")

    clusters = []
    assigned = set()

    for i, article in enumerate(recent):
        if i in assigned or len(article.tokens) < 2:
            continue
        cluster = {"articles": [article], "tokens": set(article.tokens),
                   "sources": {article.source}, "indices": {i}}
        # Find similar articles - check assigned to avoid duplicates across clusters
        for j, other in enumerate(recent):
            if j <= i or j in assigned: continue  # BUG FIX: skip already-assigned
            shared = article.tokens & other.tokens
            if len(shared) >= min_shared:
                cluster["articles"].append(other)
                cluster["tokens"] |= other.tokens
                cluster["sources"].add(other.source)
                cluster["indices"].add(j)
        # Always mark all articles in this cluster as assigned (fixes solo duplicates)
        assigned |= cluster["indices"]
        clusters.append(cluster)

    # Sort by source count descending
    clusters.sort(key=lambda c: -len(c["sources"]))
    return clusters, recent

def pick_representative(cluster):
    """Pick most informative article: investigative source > longest description."""
    arts = cluster["articles"]
    return max(arts, key=lambda a: (
        (1 if a.source in INVESTIGATIVE else 0) * 1000 +
        len(a.description) * 3 +
        len(a.title)
    ))

# ── 5W EXTRACTION ────────────────────────────────────────────────
NAME_RE = re.compile(r'\b([A-Z][a-záéíóúâêôãõ]+(?:\s+[A-Z][a-záéíóúâêôãõ]+){1,4})\b')
DATE_RE = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b')

def parse_date_br(raw):
    """Convert any date format to DD/MM/YYYY. Returns today's date on failure."""
    if not raw: return datetime.date.today().strftime("%d/%m/%Y")
    raw = raw.strip()
    # ISO: 2026-05-27T... or 2026-05-27
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m: return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    # Already BR: 27/05/2026
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw)
    if m: return raw[:10]
    # RFC 822: Wed, 27 May 2026 10:00:00 +0000
    try:
        import email.utils
        dt = email.utils.parsedate_to_datetime(raw)
        return dt.strftime("%d/%m/%Y")
    except Exception: pass
    # "27 de maio de 2026"
    _MMAP = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,
             "jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12,
             "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
             "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
    m2 = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", raw, re.I)
    if m2:
        mes = _MMAP.get(normalize(m2.group(2))[:3])
        if mes: return f"{int(m2.group(1)):02d}/{mes:02d}/{m2.group(3)}"
    # Day-of-week or unknown — fall back to today
    return datetime.date.today().strftime("%d/%m/%Y")
VALUE_RE = re.compile(r'R\$\s*[\d.,]+\s*(?:bilh|milh|mi\b|bi\b)', re.I)
PLACE_RE = re.compile(r'\b(São Paulo|Rio de Janeiro|Brasília|Brasil|SP|RJ|DF|Minas Gerais|Bahia|Goiás|Pará)\b')

PLACE_NAMES = {
    "brasilia","brasil","sao paulo","rio de janeiro","belo horizonte",
    "minas gerais","estados unidos","texas","washington","alemanha",
    "argentina","china","russia","franca","alemanha","italia",
}

# People/orgs that are clearly news actors (not places, not project names)
_NOISE_NAMES = re.compile(
    r"(Times\s+Square|Boulevard|Departamento\s+de|Ministério\s+da|"
    r"Supremo\s+Tribunal|Superior\s+Tribunal|Polícia\s+Civil|"
    r"Polícia\s+Federal|Prefeitura|Secretaria|Câmara\s+dos|"
    r"Congresso\s+Nacional)", re.I)

def extract_5w(cluster):
    rep  = pick_representative(cluster)
    desc = rep.description

    # Strip content warning sentences at start of description
    for _warn in ["Alerta:", "Alerta -", "Aviso:", "Aviso -"]:
        if desc.lower().startswith(_warn.lower()):
            _cut = desc.find(". ", len(_warn))
            if _cut > 0: desc = desc[_cut+2:].strip()
            break

    full_text = rep.title + " " + desc

    # WHO — only human names (2+ capitalized words, not a known noise pattern)
    raw_names = list(dict.fromkeys(NAME_RE.findall(full_text)))
    human_names = []
    for n in raw_names:
        if _NOISE_NAMES.search(n): continue
        if n.isupper(): continue
        # Skip if any word in the name is a known place
        words = normalize(n).split()
        if any(w in PLACE_NAMES for w in words): continue
        # Skip short names or numbers
        if len(n) < 4: continue
        human_names.append(n)
    who = ", ".join(human_names[:2]) if human_names else ""

    # WHAT — cleaned headline
    what = clean_headline(rep.title)

    # WHEN — publication date preferred over in-text date
    rep_date = parse_date_br(rep.pub_date) if rep.pub_date else ""
    when = rep_date or (DATE_RE.search(full_text).group(0) if DATE_RE.search(full_text) else "")

    # WHERE — only if specific (not just "Brasil")
    places = list(dict.fromkeys(PLACE_RE.findall(full_text)))
    where  = ", ".join(p for p in places[:2] if p not in ("Brasil","SP","RJ","DF")) or ""

    # WHY — first real sentence of description (not content warning)
    if desc and len(desc) > 30:
        # Get first sentence
        first = re.split(r"(?<=[.!?])\s+[A-ZÁÉÍÓÚ]", desc)[0].strip()
        if len(first) > 220: first = first[:217] + "…"
        why = first
    else:
        why = ""

    # VALUE
    vm = VALUE_RE.search(full_text)
    value = vm.group(0) if vm else ""

    return {"who":who,"what":what,"when":when,"where":where,"why":why,"value":value}

# ── STORY SCORING ────────────────────────────────────────────────
INVESTIGATIVE = {"Intercept","A Pública","Ag. Mural","Fiocruz","Jornal USP","Ag. Galão"}
GRANDE_IMPRENSA = {"G1","G1-SP","Folha","Estadão","O Globo","Metrópoles"}

def score_story(cluster):
    sources = cluster["sources"]
    n = len(sources)
    inv = sources & INVESTIGATIVE
    grande = sources & GRANDE_IMPRENSA
    if n >= 5:   label, emoji = "🔥 VIRAL",     "🔥"
    elif n >= 3: label, emoji = "📈 TRENDING",  "📈"
    elif n == 2: label, emoji = "📰 MÚLTIPLAS", "📰"
    else:        label, emoji = "🔍 EXCLUSIVA", "🔍"
    # If only investigative covered → worth flagging
    if n == 1 and sources <= INVESTIGATIVE:
        label, emoji = "💡 INVESTIGATIVA", "💡"
    return label, emoji, n, bool(inv), bool(grande)

STORY_CATEGORIES = [
    # (token_set, emoji, label)
    ({"corrupcao","improbidade","fraude","desvio","superfaturamento","lavagem"},
     "🔎", "Investigativo"),
    ({"privatizacao","concessao","desestatizacao","leilao","ppp","sabesp","cptm","metro"},
     "🏭", "Privatização"),
    ({"preso","prisao","pena","condenado","policia","crime","trafico","corrupcao"},
     "⚖️", "Justiça/Crime"),
    ({"stf","supremo","tribunal","juiz","juiza","liminar","decisao","sentenca"},
     "🏛️", "Judiciário"),
    ({"bolsonaro","lula","tarcisio","presidente","governador","ministro","senador"},
     "🏛️", "Política"),
    ({"economia","inflacao","juros","pib","emprego","desemprego","mercado","dolar"},
     "💰", "Economia"),
    ({"paulo","paulista","paulistano","prefeitura","covas","nunes","sp"},
     "🏙️", "São Paulo"),
    ({"saude","doenca","virus","vacina","hospital","sus","dengue","cancer"},
     "🏥", "Saúde"),
    ({"ambiental","clima","desmatamento","queimada","carbono","emissao"},
     "🌿", "Meio Ambiente"),
    ({"pesquisa","estudo","universidade","ciencia","tecnologia","descoberta"},
     "🔬", "Ciência"),
    ({"violencia","feminicidio","morte","assassinato","homicidio","estupro"},
     "🚨", "Violência"),
    ({"reforma","legislacao","camara","senado","congresso","votacao","pec"},
     "📋", "Legislativo"),
]

def story_category(cluster):
    """Assign a category label/emoji based on cluster token content."""
    tokens = cluster.get("tokens", set())
    text   = " ".join(a.title + " " + a.description for a in cluster["articles"])
    tlow   = normalize(text)
    tok_n  = set(re.findall(r"[a-zà-ÿç]{4,}", tlow))

    for token_set, emoji, label in STORY_CATEGORIES:
        if token_set & tok_n:
            return emoji, label
    return "📰", "Geral"


# ── FOLLOW-UP SUGGESTIONS ────────────────────────────────────────
FOLLOWUP_RULES = [
    # SP State govt → check DOESP (only if SP state tokens present)
    ({"privatiza","desestatiza","concessao","sabesp","cptm","metro","tarcisio","alesp","secretaria"},
     "→ Verificar DOESP: ato publicado no Diário Oficial do Estado de SP?"),
    ({"licitacao","contrato","pregao","dispensa","inexigibilidade","superfaturamento"},
     "→ Verificar TCE-SP para SP, TCU para federal"),
    # Periferias / SP social → Mural
    ({"periferia","periferias","zona leste","zona norte","zona sul","favela","comunidade","moradores"},
     "→ Ag. Mural cobre periferias paulistanas — verificar ângulo local"),
    # Health → Fiocruz
    ({"saude","doenca","epidemia","pandemia","virus","vacina","dengue"},
     "→ Fiocruz pode ter dados científicos; verificar Secretaria de Saúde SP"),
    # Education
    ({"escola","educacao","professor","universidade","mec","seduc"},
     "→ Jornal USP pode ter análise acadêmica; dados IBGE/INEP disponíveis"),
    # Corruption/investigation
    ({"corrupcao","improbidade","fraude","desvio","investigacao","tce","tcu","mp"},
     "→ A Pública e Intercept cobrem investigativamente; dados via LAI?"),
    # Congress / legislation with SP angle
    ({"reforma","pec","camara","senado","votacao","aprovado"},
     "→ Como vota bancada paulista? Impacto direto para SP?"),
    # Environment
    ({"ambiental","desmatamento","clima","queimada"},
     "→ Fiocruz e INPE têm dados científicos; CETESB para SP"),
    # Crime / violence with SP angle
    ({"violencia","homicidio","feminicidio","crime","trafico"},
     "→ Dados SSP-SP para crimes no estado; IBSP para estatísticas"),
]


def suggest_followups(cluster, sources_covered):
    all_tokens = cluster["tokens"]
    suggestions = []
    seen_sugg = set()
    for kw_set, sugg in FOLLOWUP_RULES:
        if any(k in all_tokens for k in kw_set):
            if sugg not in seen_sugg:
                suggestions.append(sugg)
                seen_sugg.add(sugg)
    # Gap analysis: major story not covered by investigative outlets
    sources = cluster["sources"]
    if len(sources) >= 3 and not (sources & INVESTIGATIVE):
        suggestions.append("→ Nenhuma fonte investigativa cobriu - ângulo em aberto para aprofundamento")
    # Major story not in SP sources
    if len(sources) >= 3 and "G1-SP" not in sources:
        sp_tokens = {"sao paulo","paulista","estado","tarcisio","capital","interior"}
        if any(t in all_tokens for t in sp_tokens):
            suggestions.append("→ G1-SP não cobriu - pode ter desdobramento local")
    return suggestions[:3]

# ── FORMATAÇÃO TELEGRAM ───────────────────────────────────────────
def fmt_sources(sources_with_links):
    """Format source list with hyperlinks."""
    parts = []
    for src_name, src_emoji, link in sources_with_links:
        if link:
            parts.append(f"{src_emoji} [{src_name}]({link})")
        else:
            parts.append(f"{src_emoji} {src_name}")
    return " · ".join(parts)

def build_story_card(cluster, rank, date_str):
    """
    Individual story card. Format inspired by DOC-SP monitor:
    - Header: rank + coverage label + category
    - Headline in bold
    - Structured fields (only when available, never empty)
    - First sentence of context
    - Sources as named links
    - Follow-up suggestions if relevant
    """
    _, em_cov, n_sources, has_inv, has_grande = score_story(cluster)
    cat_emoji, cat_label = story_category(cluster)
    w = extract_5w(cluster)
    rep = pick_representative(cluster)
    suggestions = suggest_followups(cluster, cluster["sources"])

    # ── HEADER ───────────────────────────────────────────────────
    cov_label = {1:"1 fonte",2:"2 fontes",3:"3 fontes",
                 4:"4 fontes",5:"5 fontes"}.get(n_sources, f"{n_sources} fontes")
    lines = [
        f"{em_cov} *#{rank}* | {cat_emoji} *{cat_label}* | {cov_label}",
        "━━━",
    ]

    # ── TÍTULO ───────────────────────────────────────────────────
    # Use full cleaned headline (not the short version) in cards
    headline = w["what"]
    if len(headline) > 140: headline = headline[:137] + "…"
    lines.append(f"📌 *{headline}*")

    # ── CAMPOS ESTRUTURADOS (só exibe se tiver dado real) ────────
    # WHO — only if it's a genuine person name (not place/project)
    if w["who"] and len(w["who"]) > 3:
        lines.append(f"👤 {w['who']}")

    # WHERE — only if different from "São Paulo" / "Brasil" defaults
    if w["where"] and w["where"] not in ("SP","RJ","DF","Brasil"):
        lines.append(f"📍 {w['where']}")

    # DATE — only show if it's a real date (not "quarta-feira")
    if w["when"] and re.match(r"\d{2}/\d{2}/\d{4}", w["when"]):
        lines.append(f"📅 {w['when']}")

    # VALUE — always show if present
    if w["value"]:
        lines.append(f"💰 {w['value']}")

    # ── CONTEXTO (primeira frase real) ───────────────────────────
    if w["why"] and len(w["why"]) > 30:
        lines.append(f"💬 _{w['why']}_")

    # ── FONTES ───────────────────────────────────────────────────
    lines.append("━━━")
    src_parts = []
    seen_src  = set()
    for art in cluster["articles"]:
        if art.source not in seen_src:
            seen_src.add(art.source)
            if art.link:
                src_parts.append(f"[{art.emoji} {art.source}]({art.link})")
            else:
                src_parts.append(f"{art.emoji} {art.source}")
        if len(src_parts) >= 5: break
    lines.append("📱 " + " · ".join(src_parts))

    # ── GAP + DESDOBRAMENTOS ─────────────────────────────────────
    gaps = []
    if n_sources >= 3 and not has_inv:
        gaps.append("Nenhuma fonte investigativa cobriu — ângulo em aberto")
    if n_sources == 1 and has_inv:
        gaps.append("Só fonte especializada cobriu — grande imprensa ainda não")

    if suggestions or gaps:
        lines.append("━━━")
        lines.append("💡 *Desdobramentos:*")
        for g in gaps[:1]:
            lines.append(f"  → {g}")
        for s in suggestions[:2]:
            lines.append(f"  {s}")

    return "\n".join(lines)


def build_summary(clusters, all_articles, date_str, failed_sources):
    """
    First message: compact overview with readable headlines, max 6 stories.
    Each line: coverage emoji + category + short headline + source count + linked icons.
    """
    n_ok     = len(SOURCES) - len(failed_sources)
    n_arts   = len(all_articles)
    viral    = sum(1 for c in clusters if len(c["sources"]) >= 5)
    trending = sum(1 for c in clusters if 3 <= len(c["sources"]) <= 4)
    multi    = sum(1 for c in clusters if len(c["sources"]) == 2)
    exclus   = sum(1 for c in clusters if len(c["sources"]) == 1)

    lines = [
        f"📰 *MONITOR — {date_str}*",
        f"🗞️ {n_ok}/{len(SOURCES)} fontes · {n_arts} pautas relevantes",
        f"🔥 {viral} viral  📈 {trending} trending  📰 {multi} múltiplas  🔍 {exclus} exclusivas",
        "━━━",
    ]

    for i, c in enumerate(clusters[:6], 1):
        _, em, n, has_inv, _ = score_story(c)
        cat_em, _            = story_category(c)
        rep                  = pick_representative(c)
        headline             = short_headline(rep.title, rep.description)
        inv_flag             = " 💡" if has_inv else ""

        # Source icons with links (max 4)
        src_links = []
        seen = set()
        for art in c["articles"]:
            if art.source not in seen:
                seen.add(art.source)
                src_links.append(f"[{art.emoji}]({art.link})" if art.link else art.emoji)
            if len(src_links) >= 4: break

        lines.append(f"{em} {cat_em} *{i}.* {headline}{inv_flag} · {n}f  " + " ".join(src_links))

    if len(clusters) > 6:
        lines.append(f"_↓ mais {len(clusters)-6} pautas nos cards abaixo_")
    if failed_sources:
        lines.append("━━━")
        lines.append("⚠️ Sem resposta: " + ", ".join(failed_sources))
    return "\n".join(lines)


def build_uncovered(solo_clusters, date_str):
    """Digest of single-source stories — investigative first, then big press."""
    inv_solos = [c for c in solo_clusters if c["sources"] & INVESTIGATIVE]
    big_solos = [c for c in solo_clusters if c["sources"] & GRANDE_IMPRENSA
                 and not (c["sources"] & INVESTIGATIVE)]

    lines = [
        f"🔍 *EXCLUSIVAS — {date_str}*",
        f"_{len(inv_solos)} investigativas · {len(big_solos)} grande imprensa_",
        "━━━",
    ]
    if inv_solos:
        lines.append("*Investigativas / especializadas:*")
        for c in inv_solos[:8]:
            rep = pick_representative(c)
            src = list(c["sources"])[0]
            emo = next((s["emoji"] for s in SOURCES if s["name"] == src), "")
            cat_em, _ = story_category(c)
            t = clean_headline(rep.title)[:70] + ("…" if len(rep.title) > 70 else "")
            lnk = f"[{emo} {src}]({rep.link})" if rep.link else f"{emo} {src}"
            lines.append(f"  {cat_em} {lnk}: _{t}_")

    if big_solos:
        lines.append("\n*Grande imprensa (exclusivos):*")
        for c in big_solos[:5]:
            rep = pick_representative(c)
            src = list(c["sources"])[0]
            emo = next((s["emoji"] for s in SOURCES if s["name"] == src), "")
            cat_em, _ = story_category(c)
            t = clean_headline(rep.title)[:70] + ("…" if len(rep.title) > 70 else "")
            lnk = f"[{emo} {src}]({rep.link})" if rep.link else f"{emo} {src}"
            lines.append(f"  {cat_em} {lnk}: _{t}_")

    return "\n".join(lines)


# ── TELEGRAM ─────────────────────────────────────────────────────
_last_send = 0.0
def send_telegram(text, silent=False):
    global _last_send
    gap = time.time() - _last_send
    if gap < 2.0: time.sleep(2.0 - gap)
    for _ in range(3):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown",
                      "disable_web_page_preview": True, "disable_notification": silent},
                timeout=15)
            _last_send = time.time()
            if r.status_code == 200: return True
            if r.status_code == 429:
                time.sleep(r.json().get("parameters",{}).get("retry_after",30)+1); continue
            print(f"  TG {r.status_code}"); return False
        except Exception as e: print(f"  TG err: {e}"); time.sleep(3)
    return False

def split_long(text, mx=3800):
    if len(text) <= mx: return [text]
    parts = []; cur = ""
    for l in text.split("\n"):
        if len(cur)+len(l)+1 > mx: parts.append(cur); cur = l
        else: cur += ("\n" if cur else "") + l
    if cur: parts.append(cur)
    return parts


# ═══════════════════════════════════════════════════════════════════
# INSTITUTIONAL SOURCES — Primary releases from gov institutions
# Different from news: date-filtered, no clustering, own message
# ═══════════════════════════════════════════════════════════════════

INST_SOURCES = [
    # ── BCB — Angular SPA, Playwright intercepts internal API ────
    {
        "name": "BCB",
        "full": "Banco Central do Brasil",
        "emoji": "🏦", "tier": "federal",
        "url":   "https://www.bcb.gov.br/noticias",   # Playwright navigates here
        "api":   "https://www.bcb.gov.br/api/servico/sitebcb/noticias?quantidade=20",
        "fmt":   "bcb_playwright",                    # Playwright + API intercept
        "home":  "https://www.bcb.gov.br/noticias",
    },
    # ── IBGE — WordPress, use feed ───────────────────────────────
    {
        "name": "IBGE",
        "full": "Instituto Brasileiro de Geografia e Estatística",
        "emoji": "📊", "tier": "federal",
        "url":  "https://agenciadenoticias.ibge.gov.br/feed",
        "rss2": "https://agenciadenoticias.ibge.gov.br/?feed=rss2",
        "fmt":  "rss",
        "home": "https://agenciadenoticias.ibge.gov.br/",
    },
    # ── gov.br Plone sites — RSS at {news-url}/RSS ──────────────
    {
        "name": "ANVISA",
        "full": "Agência Nacional de Vigilância Sanitária",
        "emoji": "🏥", "tier": "federal",
        "url":  "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/RSS",
        "rss2": "https://www.gov.br/anvisa/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa",
    },
    {
        "name": "ANS",
        "full": "Agência Nacional de Saúde Suplementar",
        "emoji": "🏥", "tier": "federal",
        "url":  "https://www.gov.br/ans/pt-br/assuntos/noticias/RSS",
        "rss2": "https://www.gov.br/ans/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/ans/pt-br/assuntos/noticias",
    },
    {
        "name": "CADE",
        "full": "Conselho Administrativo de Defesa Econômica",
        "emoji": "⚖️", "tier": "federal",
        "url":  "https://www.gov.br/cade/pt-br/assuntos/noticias/RSS",
        "rss2": "https://www.gov.br/cade/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/cade/pt-br/assuntos/noticias",
    },
    {
        "name": "CVM",
        "full": "Comissão de Valores Mobiliários",
        "emoji": "📈", "tier": "federal",
        "url":  "https://www.gov.br/cvm/pt-br/assuntos/noticias/RSS",
        "rss2": "https://www.gov.br/cvm/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/cvm/pt-br/assuntos/noticias",
    },
    {
        "name": "Receita Federal",
        "full": "Secretaria Especial da Receita Federal do Brasil",
        "emoji": "💼", "tier": "federal",
        "url":  "https://www.gov.br/receitafederal/pt-br/noticias/RSS",
        "rss2": "https://www.gov.br/receitafederal/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/receitafederal/pt-br/noticias",
    },
    {
        "name": "INPE",
        "full": "Instituto Nacional de Pesquisas Espaciais",
        "emoji": "🌿", "tier": "federal",
        "url":  "https://www.gov.br/inpe/pt-br/assuntos/ultimas-noticias/RSS",
        "rss2": "https://www.gov.br/inpe/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/inpe/pt-br/assuntos/ultimas-noticias",
    },
    {
        "name": "IBAMA",
        "full": "Instituto Brasileiro do Meio Ambiente",
        "emoji": "🌿", "tier": "federal",
        "url":  "https://www.gov.br/ibama/pt-br/noticias/RSS",
        "rss2": "https://www.gov.br/ibama/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/ibama/pt-br/noticias",
    },
    {
        "name": "ANEEL",
        "full": "Agência Nacional de Energia Elétrica",
        "emoji": "⚡", "tier": "federal",
        "url":  "https://www.gov.br/aneel/pt-br/assuntos/noticias/RSS",
        "rss2": "https://www.gov.br/aneel/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/aneel/pt-br/assuntos/noticias",
    },
    {
        "name": "ANTT",
        "full": "Agência Nacional de Transportes Terrestres",
        "emoji": "🚛", "tier": "federal",
        "url":  "https://www.gov.br/antt/pt-br/assuntos/noticias/RSS",
        "rss2": "https://www.gov.br/antt/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/antt/pt-br/assuntos/noticias",
    },
    {
        "name": "AGU",
        "full": "Advocacia-Geral da União",
        "emoji": "🏛️", "tier": "federal",
        "url":  "https://www.gov.br/agu/pt-br/comunicacao/noticias/RSS",
        "rss2": "https://www.gov.br/agu/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/agu/pt-br/comunicacao/noticias",
    },
    # ── Custom CMS ───────────────────────────────────────────────
    {
        "name": "TCU",
        "full": "Tribunal de Contas da União",
        "emoji": "🔍", "tier": "federal",
        "url":  "https://portal.tcu.gov.br/imprensa/noticias/rss.xml",
        "rss2": "https://portal.tcu.gov.br/rss/tcu-noticias.rss",
        "fmt":  "rss",
        "home": "https://portal.tcu.gov.br/imprensa/noticias/",
    },
    {
        "name": "STF",
        "full": "Supremo Tribunal Federal",
        "emoji": "⚖️", "tier": "federal",
        "url":  "https://portal.stf.jus.br/noticias/rss.asp",
        "fmt":  "rss",
        "home": "https://portal.stf.jus.br/noticias/",
    },
    # ── São Paulo (estadual) — WordPress ─────────────────────────
    {
        "name": "TCE-SP",
        "full": "Tribunal de Contas do Estado de São Paulo",
        "emoji": "🔍", "tier": "estadual",
        "url":  "https://www.tce.sp.gov.br/feed",
        "rss2": "https://www.tce.sp.gov.br/sites/default/files/feeds/noticias.xml",
        "fmt":  "rss",
        "home": "https://www.tce.sp.gov.br/",
    },
    {
        "name": "Seade",
        "full": "Fundação Sistema Estadual de Análise de Dados",
        "emoji": "📊", "tier": "estadual",
        "url":  "https://www.seade.gov.br/wp-json/wp/v2/posts?per_page=15&orderby=date&order=desc&_fields=title,link,excerpt,date",
        "rss2": "https://www.seade.gov.br/feed/",
        "fmt":  "wp_api",
        "home": "https://www.seade.gov.br/noticias/",
    },
    {
        "name": "Agência SP",
        "full": "Agência de Notícias do Governo do Estado de SP",
        "emoji": "🏛️", "tier": "estadual",
        "url":  "https://www.agenciasp.sp.gov.br/wp-json/wp/v2/posts?per_page=15&orderby=date&order=desc&_fields=title,link,excerpt,date",
        "rss2": "https://www.agenciasp.sp.gov.br/feed/",
        "fmt":  "wp_api",
        "home": "https://www.agenciasp.sp.gov.br/",
    },
]

# ── INSTITUTIONAL PARSERS ─────────────────────────────────────────

def _parse_bcb(text):
    """BCB API: {value: [{Titulo, Resumo, DataPublicacao, Url, Categoria}]}"""
    items = []
    try:
        data = json.loads(text)
        raw = data.get("value") or data.get("conteudo") or []
        if isinstance(data, list): raw = data
        for item in raw:
            title = item.get("Titulo","") or item.get("titulo","")
            desc  = item.get("Resumo","") or item.get("resumo","") or item.get("Introducao","")
            date  = item.get("DataPublicacao","") or item.get("dataPublicacao","")
            url   = item.get("Url","") or item.get("url","") or item.get("Link","")
            cat   = item.get("Categoria","") or item.get("categoria","")
            if url and not url.startswith("http"):
                url = "https://www.bcb.gov.br" + url
            if title: items.append({"title":title,"desc":clean_html(desc),"date":date,"url":url,"cat":cat})
    except (json.JSONDecodeError, KeyError, TypeError): pass
    return items

def _parse_ibge(text):
    """IBGE API: [{id, tipo, titulo, introducao, data_publicacao, link}]"""
    items = []
    try:
        raw = json.loads(text)
        if isinstance(raw, dict): raw = raw.get("items") or raw.get("data") or []
        for item in raw:
            title = item.get("titulo","") or item.get("title","")
            desc  = item.get("introducao","") or item.get("summary","")
            date  = item.get("data_publicacao","") or item.get("data","") or ""
            url   = item.get("link","") or item.get("url","")
            tipo  = item.get("tipo","") or item.get("type","")
            if not url and item.get("id"):
                url = f"https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/{item['id']}"
            if title: items.append({"title":title,"desc":clean_html(desc),"date":date,"url":url,"cat":tipo})
    except (json.JSONDecodeError, KeyError, TypeError): pass
    return items

def _parse_plone_rss(text):
    """Plone @@rss.xml — standard Atom/RSS used by gov.br sites."""
    return _parse_xml_feed(text.encode() if isinstance(text,str) else text, "", "")

def _item_is_today(date_str, hoje):
    """Check if a date string refers to today."""
    if not date_str: return True  # assume today if no date
    d = parse_date_br(date_str)
    if d and d[:10] == hoje.strftime("%d/%m/%Y")[:10]: return True
    # Some APIs return "28/05/2026 09:00:00"
    if d and hoje.strftime("%d/%m") in d: return True
    return False

def fetch_institutional(source, session, hoje):
    """
    Fetch a single institutional source.
    Strategy by fmt:
      - rss / wp_api: direct HTTP with session
      - bcb_playwright: Playwright navigates to BCB, intercepts internal API
    Returns list of today's items [{title, desc, date, url, cat}].
    """
    name  = source["name"]
    fmt   = source.get("fmt","rss")

    # BCB needs Playwright to get cookies and intercept the internal API
    if fmt == "bcb_playwright":
        return _fetch_bcb_playwright(source, hoje)

    # All other sources: try URLs in order
    urls = [source.get("url",""), source.get("rss2",""), source.get("rss3","")]
    urls = [u for u in urls if u]

    for url in urls:
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200:
                print(f"  {name}: HTTP {r.status_code}  [{url[-45:]}]")
                continue
            raw = _parse_inst_response(r, source)
            if raw:
                today = [i for i in raw if _item_is_today(i.get("date",""), hoje)]
                # If nothing today but very early morning, allow yesterday too
                if not today:
                    import datetime as _dt
                    yesterday = hoje - _dt.timedelta(days=1)
                    today = [i for i in raw if _item_is_today(i.get("date",""), yesterday)]
                    if today: print(f"  {name}: {len(today)} de ontem (tolerância horária)")
                print(f"  {name}: {len(raw)} total → {len(today)} hoje ✅")
                return today
            else:
                print(f"  {name}: 200 mas 0 itens  [{url[-45:]}]")
        except Exception as e:
            print(f"  {name}: {e.__class__.__name__}: {str(e)[:50]}")

    # Playwright fallback
    return _fetch_inst_playwright(source, hoje)


def _parse_inst_response(r, source):
    """Parse institutional HTTP response — RSS/Atom, WP API, Plone RSS, or JSON."""
    fmt  = source.get("fmt","rss")
    text = r.text
    body = r.content

    # WordPress REST API
    if fmt == "wp_api" or "/wp-json/" in r.url:
        arts = _parse_wp_api(text, source["name"], source["emoji"])
        return [{"title":a.title,"desc":a.description,"date":a.pub_date,"url":a.link,"cat":""} for a in arts]

    # BCB JSON
    if fmt == "bcb":
        return _parse_bcb(text)

    # IBGE JSON
    if fmt == "ibge":
        return _parse_ibge(text)

    # RSS / Atom (default — covers Plone RSS, standard RSS, Atom)
    arts = _parse_xml_feed(body, source["name"], source["emoji"])
    if arts:
        return [{"title":a.title,"desc":a.description,"date":a.pub_date,"url":a.link,"cat":""} for a in arts]

    # HTML fallback
    if "<html" in text.lower()[:200]:
        arts = _parse_html_feed(text, source["name"], source["emoji"])
        return [{"title":a.title,"desc":a.description,"date":a.pub_date,"url":a.link,"cat":""} for a in arts]

    return []


def _fetch_bcb_playwright(source, hoje):
    """
    Use Playwright to navigate BCB news page and intercept the internal API call.
    BCB is an Angular SPA — the news list is loaded via an internal REST API.
    """
    items = []
    try:
        from playwright.sync_api import sync_playwright
        intercept_data = [None]

        def on_response(response):
            if "api/servico/sitebcb/noticias" in response.url and response.status == 200:
                try:
                    data = response.json()
                    raw  = _parse_bcb(response.text())
                    intercept_data[0] = raw
                except Exception: pass

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"])
            ctx  = browser.new_context(user_agent=UA, locale="pt-BR")
            page = ctx.new_page()
            page.on("response", on_response)
            page.goto(source["url"], wait_until="networkidle", timeout=25000)
            page.wait_for_timeout(4000)

            # Fallback: extract from DOM if API intercept failed
            if not intercept_data[0]:
                articles = []
                for sel in ["h3.noticias-titulo a","div.noticias-card a[href*='/detalhenoticia']",
                            ".card-noticia a",".noticia-titulo a","article a"]:
                    els = page.query_selector_all(sel)
                    if els:
                        for el in els[:20]:
                            try:
                                title = (el.inner_text() or "").strip()
                                href  = el.get_attribute("href") or ""
                                if not href.startswith("http"): href = "https://www.bcb.gov.br" + href
                                if title and len(title) > 10:
                                    articles.append({"title":title,"desc":"","date":"","url":href,"cat":""})
                            except: pass
                        if articles: break
                intercept_data[0] = articles

            ctx.close(); browser.close()

        raw = intercept_data[0] or []
        today = [i for i in raw if _item_is_today(i.get("date",""), hoje)] if raw else raw
        print(f"  BCB (Playwright): {len(raw)} total → {len(today)} hoje ✅")
        return today

    except ImportError:
        print("  BCB: Playwright não disponível")
        return []
    except Exception as e:
        print(f"  BCB Playwright err: {e}")
        return []


def _fetch_inst_playwright(source, hoje):
    """Generic Playwright fallback for institutional sites."""
    items = []
    try:
        from playwright.sync_api import sync_playwright
        from urllib.parse import urljoin

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"])
            ctx  = browser.new_context(user_agent=UA, locale="pt-BR")
            page = ctx.new_page()
            page.goto(source.get("home", source.get("url","")),
                      wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(3000)

            for sel in ["article a h2","article h2","h2.titulo",".noticia a",
                        ".summary-title a",".tile-summary h2",".list-item h3",
                        "h3.entry-title","h2.post-title","a h2","a h3"]:
                els = page.query_selector_all(sel)
                if els:
                    for el in els[:15]:
                        try:
                            title = (el.inner_text() or "").strip()
                            parent = el.evaluate_handle("el => el.closest('a')")
                            href = (parent.get_attribute("href") or "") if parent else ""
                            if not href.startswith("http"): href = urljoin(source.get("home",""), href)
                            if title and len(title) > 15:
                                items.append({"title":title,"desc":"","date":"","url":href,"cat":""})
                        except: pass
                    if items: break

            ctx.close(); browser.close()

        today = [i for i in items if _item_is_today(i.get("date",""), hoje)]
        if items:
            print(f"  {source['name']} (PW fallback): {len(items)} total → {len(today)} hoje")
        return today

    except Exception as e:
        print(f"  {source['name']} PW fallback err: {e}")
        return []


def build_inst_message(inst_results, date_str):
    """
    Build the institutional releases Telegram message.
    Format: one block per institution, clean title + context + link.
    """
    total = sum(len(v) for v in inst_results.values())
    if total == 0:
        return ""

    lines = [
        f"🏛️ *FONTES INSTITUCIONAIS — {date_str}*",
        f"_{total} publicações hoje_",
        "━━━",
    ]

    # Group by tier: federal first, then estadual
    for tier_label, tier in [("Federal", "federal"), ("São Paulo", "estadual")]:
        tier_sources = [s for s in INST_SOURCES if s.get("tier") == tier
                        and inst_results.get(s["name"])]
        if not tier_sources: continue

        lines.append(f"*{tier_label}:*")
        for src in tier_sources:
            name  = src["name"]
            emoji = src["emoji"]
            items = inst_results.get(name, [])
            if not items: continue

            for item in items[:3]:  # max 3 per source
                title = item.get("title","").strip()
                desc  = item.get("desc","").strip()
                url   = item.get("url","").strip()
                cat   = item.get("cat","")

                # Clean title
                title = clean_headline(title)[:100]
                if cat: title = f"[{cat}] {title}"

                # Context: first sentence only
                ctx = ""
                if desc:
                    fse = desc.find(". ")
                    ctx = (desc[:fse+1] if 20 < fse < 160 else desc[:120]).strip()

                lnk = f"[{emoji} {name}]({url})" if url else f"{emoji} {name}"
                lines.append(f"  {lnk}: *{title}*")
                if ctx: lines.append(f"  _{ctx}_")

        lines.append("")  # spacing

    return "\n".join(lines).strip()


# ── MAIN ─────────────────────────────────────────────────────────
def main():
    now      = datetime.datetime.now(datetime.timezone.utc).astimezone(
                    datetime.timezone(datetime.timedelta(hours=-3)))  # BRT
    date_str = now.strftime("%d/%m/%Y")
    time_str = now.strftime("%H:%M")
    run_str  = f"{date_str} {time_str}"
    print(f"=== Monitor de Notícias v1.0 - {run_str} BRT ===\n")

    # 0. Fetch institutional sources (date-filtered, separate message)
    print("\n  Fontes institucionais...")
    inst_session = requests.Session()
    inst_session.headers.update({"User-Agent": UA, "Accept": "*/*", "Accept-Language": "pt-BR,pt;q=0.9"})
    inst_results = {}
    _hoje_date = now.date()
    for isrc in INST_SOURCES:
        releases = fetch_institutional(isrc, inst_session, _hoje_date)
        inst_results[isrc["name"]] = releases

    # 1. Fetch all sources in parallel (max 6 workers)
    # Playwright sources are serialised inside fetch_rss to share browser
    all_articles = []
    failed = []
    print("  Coletando fontes em paralelo...")
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fetch_rss, src): src for src in SOURCES}
        for future in as_completed(futures):
            src  = futures[future]
            try:
                arts = future.result()
            except Exception as e:
                print(f"  {src['name']} exc: {e}")
                arts = []
            if arts:
                all_articles.extend(arts)
            else:
                failed.append(src["name"])

    total = len(all_articles)
    print(f"\n  Total RSS: {total} artigos de {len(SOURCES)-len(failed)}/{len(SOURCES)} fontes")

    # Playwright batch: all sources that failed RSS share one browser
    pw_sources = [s for s in SOURCES if s.get("_needs_playwright")]
    if pw_sources:
        print(f"  Playwright: {len(pw_sources)} fontes...")
        pw_arts = _playwright_scrape_batch(pw_sources)
        for src_name, arts in pw_arts.items():
            if arts:
                all_articles.extend(arts)
                if src_name in failed: failed.remove(src_name)
                print(f"  {src_name}: {len(arts)} artigos via Playwright ✅")
    total = len(all_articles)
    print(f"  Total final: {total} artigos de {len(SOURCES)-len(failed)}/{len(SOURCES)} fontes")

    if total < 5:
        send_telegram(f"⚠️ Monitor de Notícias {date_str}: apenas {total} artigos coletados. Verificar feeds.")
        return

    # 2. Cluster
    print("\n  Clusterizando...")
    # Use shorter window for afternoon/evening runs to avoid morning repeats
    hour_utc = datetime.datetime.utcnow().hour
    window_h = 24 if hour_utc <= 12 else 10  # 7h run: 24h; 12h/17h/20h: 10h
    clusters, recent = cluster_articles(all_articles, min_shared=2, window_hours=window_h)
    multi_source  = [c for c in clusters if len(c["sources"]) >= 2]
    single_source = [c for c in clusters if len(c["sources"]) == 1]
    print(f"  Clusters multi-fonte: {len(multi_source)}")
    print(f"  Histórias exclusivas: {len(single_source)}")

    # 3. Send summary
    summary = build_summary(multi_source + single_source[:5], recent, run_str, failed)
    send_telegram(summary)
    time.sleep(1)

    # 4. Send top trending story cards (most covered first)
    print("\n  Enviando fichas...")
    sent = 0
    for rank, cluster in enumerate(multi_source[:12], 1):
        card = build_story_card(cluster, rank, date_str)
        for part in split_long(card):
            send_telegram(part)
        time.sleep(0.5)
        sent += 1

    # 5. Exclusivas / investigativas digest
    if any(c["sources"] & INVESTIGATIVE for c in single_source):
        uncov = build_uncovered(single_source, date_str)
        for part in split_long(uncov):
            send_telegram(part, silent=True)

    print(f"\n  Done. {sent} fichas enviadas.")

    # Institutional releases (separate message, silent)
    inst_msg = build_inst_message(inst_results, run_str)
    if inst_msg:
        for part in split_long(inst_msg):
            send_telegram(part, silent=False)

if __name__ == "__main__":
    main()
