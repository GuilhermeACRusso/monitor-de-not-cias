"""
Monitor de Notícias v1.0 — Análise de cobertura jornalística brasileira
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
from collections import defaultdict, Counter

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
     "rss":  "https://agenciamural.org.br/feed/",
     "rss2": "https://agenciamural.org.br/noticias/",
     "home": "https://agenciamural.org.br/noticias/",
     "fmt":  "rss"},
    # Científica / acadêmica
    {"name": "Fiocruz",      "emoji": "🏥", "tier": "cientifica",
     "rss":  "https://agencia.fiocruz.br/feed/",
     "rss2": "https://agencia.fiocruz.br/noticias",
     "home": "https://agencia.fiocruz.br/",
     "fmt":  "rss"},
    {"name": "Jornal USP",   "emoji": "🎓", "tier": "cientifica",
     "rss":  "https://jornal.usp.br/feed/",
     "home": "https://jornal.usp.br/",
     "fmt":  "rss"},
    {"name": "Ag. Galão",    "emoji": "🎭", "tier": "cultural",
     "rss":  "https://agenciagalo.com/feed/",
     "rss2": "https://agenciagalo.com/",
     "home": "https://agenciagalo.com/",
     "fmt":  "rss"},
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
        self.title       = title.strip()
        self.link        = link.strip() if link else ""
        self.description = clean_html(description or "")[:500]
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
    urls  = [source.get("rss",""), source.get("rss2","")]
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
                    # Debug: show what we got
                    snippet = ' '.join(r.text[:120].split())

                    print(f"  {name}: 200 mas 0 itens — body: {snippet}")
            else:
                print(f"  {name}: HTTP {r.status_code}  [{url[-45:]}]")
        except Exception as e:
            print(f"  {name}: {e.__class__.__name__}: {str(e)[:60]}")

    # Playwright fallback for sites that block requests but work in a browser
    arts = _playwright_scrape(source)
    if arts:
        print(f"  {name}: {len(arts)} artigos via Playwright ✅")
        return arts

    print(f"  {name}: ❌ sem artigos")
    return []


def _parse_response(r, source):
    """Parse HTTP response — detect format and extract articles."""
    name  = source["name"]
    emoji = source["emoji"]
    fmt   = source.get("fmt","rss")
    ct    = r.headers.get("content-type","").lower()
    body  = r.content
    text  = r.text

    # ── JSON detection ───────────────────────────────────────
    is_json = "json" in ct or text.lstrip().startswith("{") or text.lstrip().startswith("[")
    if is_json or fmt == "globo_json":
        return _parse_json_feed(text, name, emoji)

    # ── XML / RSS / Atom ─────────────────────────────────────
    is_xml = ("xml" in ct or "rss" in ct or
              text.lstrip()[:5] in ("<rss ", "<?xml", "<feed"))
    if is_xml:
        arts = _parse_xml_feed(body, name, emoji)
        if arts: return arts

    # ── HTML fallback (try to find structured data or links) ─
    if "html" in ct or "<html" in text.lower()[:200]:
        return _parse_html_feed(text, name, emoji)

    return []


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


def _playwright_scrape(source):
    """Playwright fallback for JS-heavy sites. Returns articles or []."""
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
    Two articles are in the same cluster if they share ≥ min_shared significant tokens.
    Returns list of clusters sorted by number of sources.
    """
    recent = [a for a in articles if a.is_recent(window_hours)]
    print(f"\n  Artigos recentes ({window_hours}h): {len(recent)}/{len(articles)}")

    clusters = []
    assigned = set()

    for i, article in enumerate(recent):
        if i in assigned or len(article.tokens) < 2:
            continue
        cluster = {"articles": [article], "tokens": set(article.tokens),
                   "sources": {article.source}, "indices": {i}}
        # Find similar articles
        for j, other in enumerate(recent):
            if j <= i or j in assigned: continue
            shared = article.tokens & other.tokens
            if len(shared) >= min_shared:
                cluster["articles"].append(other)
                cluster["tokens"] |= other.tokens
                cluster["sources"].add(other.source)
                cluster["indices"].add(j)
        if len(cluster["articles"]) > 1:
            assigned |= cluster["indices"]
        clusters.append(cluster)

    # Sort by source count descending
    clusters.sort(key=lambda c: -len(c["sources"]))
    return clusters, recent

def pick_representative(cluster):
    """Pick the most informative article from a cluster."""
    arts = cluster["articles"]
    # Prefer longest description, then longest title
    return max(arts, key=lambda a: len(a.description)*3 + len(a.title))

# ── 5W EXTRACTION ────────────────────────────────────────────────
NAME_RE = re.compile(r'\b([A-Z][a-záéíóúâêôãõ]+(?:\s+[A-Z][a-záéíóúâêôãõ]+){1,4})\b')
DATE_RE = re.compile(r'\b(\d{1,2}[/\-]\d{1,2}(?:[/\-]\d{2,4})?|\b(?:segunda|terça|quarta|quinta|sexta|sábado|domingo)(?:-feira)?)\b', re.I)
VALUE_RE = re.compile(r'R\$\s*[\d.,]+\s*(?:bilh|milh|mi\b|bi\b)', re.I)
PLACE_RE = re.compile(r'\b(São Paulo|Rio de Janeiro|Brasília|Brasil|SP|RJ|DF|Minas Gerais|Bahia|Goiás|Pará)\b')

def extract_5w(cluster):
    rep = pick_representative(cluster)
    full_text = rep.title + " " + rep.description

    who   = ", ".join(list(dict.fromkeys(NAME_RE.findall(full_text)))[:3]) or "—"
    what  = rep.title
    when  = DATE_RE.search(full_text)
    when  = when.group(0) if when else datetime.date.today().strftime("%d/%m/%Y")
    where = ", ".join(list(dict.fromkeys(PLACE_RE.findall(full_text)))[:2]) or "Brasil"
    why   = (rep.description[:200] + "…") if len(rep.description) > 200 else rep.description or "—"
    value = VALUE_RE.search(full_text)
    value = value.group(0) if value else ""
    return {"who":who, "what":what, "when":when, "where":where, "why":why, "value":value}

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

# ── FOLLOW-UP SUGGESTIONS ────────────────────────────────────────
FOLLOWUP_RULES = [
    # (keywords_in_cluster, suggestion)
    ({"privatiza","desestatiza","concessao","sabesp","metro","cptm"},
     "→ Verificar DOESP: concessão/privatização → publicação formal confirmada?"),
    ({"tarcisio","governador","governo","estado","sao paulo","paulista"},
     "→ Cruzar com DOESP: ato publicado no diário oficial do estado?"),
    ({"licitacao","contrato","pregao","dispensa","inexigibilidade"},
     "→ Verificar TCE-SP e DOESP: extrato de contrato publicado?"),
    ({"policia","policial","morte","letalidade","operacao","seguranca"},
     "→ Ag. Mural costuma cobrir periferias; A Pública tem dados de letalidade"),
    ({"saude","hospital","leito","sus","vacina","dengue","medicamento"},
     "→ Fiocruz pode ter dados científicos; verificar regulação ANVISA"),
    ({"escola","educacao","professor","universidade","mec","seduc"},
     "→ Jornal USP pode ter análise acadêmica; verificar dados IBGE/INEP"),
    ({"corrupcao","improbidade","fraude","desvio","superfaturamento"},
     "→ A Pública e Intercept cobrem investigativamente; dados via LAI?"),
    ({"reforma","pec","camara","senado","votacao","aprovado"},
     "→ Impacto para SP: como vota bancada paulista? Tarcísio se posicionou?"),
    ({"ambiental","desmatamento","clima","enchente","queimada"},
     "→ Fiocruz e ISA têm cobertura científica; dados INPE disponíveis"),
    ({"habitacao","moradia","sem-teto","cdhu","cohab"},
     "→ Ag. Mural cobre periferias SP; verificar CDHU no DOESP"),
    ({"cultural","sesc","arte","museu","teatro","patrimonio"},
     "→ Ag. Galão cobre cultura SP; SESC Pompeia é referência"),
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
        suggestions.append("→ Nenhuma fonte investigativa cobriu — ângulo em aberto para aprofundamento")
    # Major story not in SP sources
    if len(sources) >= 3 and "G1-SP" not in sources:
        sp_tokens = {"sao paulo","paulista","estado","tarcisio","capital","interior"}
        if any(t in all_tokens for t in sp_tokens):
            suggestions.append("→ G1-SP não cobriu — pode ter desdobramento local")
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
    label, emoji, n_sources, has_inv, has_grande = score_story(cluster)
    w = extract_5w(cluster)
    rep = pick_representative(cluster)
    suggestions = suggest_followups(cluster, cluster["sources"])

    # Header
    lines = [
        f"{emoji} *{label}* #{rank} — {n_sources}/{len(SOURCES)} fontes",
        f"━━━",
    ]

    # 5W block
    title_short = w["what"][:120] + ("…" if len(w["what"])>120 else "")
    lines.append(f"📌 *{title_short}*")
    if w["who"] and w["who"] != "—":
        lines.append(f"👤 {w['who'][:80]}")
    if w["where"] and w["where"] != "Brasil":
        lines.append(f"📍 {w['where']}")
    lines.append(f"📅 {w['when']}")
    if w["value"]:
        lines.append(f"💰 {w['value']}")
    if w["why"] and w["why"] != "—" and len(w["why"]) > 20:
        why_short = w["why"][:180] + ("…" if len(w["why"])>180 else "")
        lines.append(f"💬 _{why_short}_")

    # Sources with links
    lines.append("━━━")
    source_list = []
    for art in cluster["articles"]:
        source_list.append((art.source, art.emoji, art.link))
    # Deduplicate by source name, keep first link
    seen_src = {}
    for sn, se, sl in source_list:
        if sn not in seen_src: seen_src[sn] = (se, sl)
    src_formatted = [(sn, se, sl) for sn,(se,sl) in seen_src.items()]
    lines.append(f"📱 {fmt_sources(src_formatted)}")

    # Follow-up suggestions
    if suggestions:
        lines.append("━━━")
        lines.append("💡 *Sugestões de pauta:*")
        for s in suggestions:
            lines.append(s)

    return "\n".join(lines)

def build_summary(clusters, all_articles, date_str, failed_sources):
    n_sources_ok = len(SOURCES) - len(failed_sources)
    n_articles = len(all_articles)
    viral    = sum(1 for c in clusters if len(c["sources"])>=5)
    trending = sum(1 for c in clusters if 3<=len(c["sources"])<=4)
    multi    = sum(1 for c in clusters if len(c["sources"])==2)
    exclus   = sum(1 for c in clusters if len(c["sources"])==1)

    lines = [
        f"📰 *MONITOR DE NOTÍCIAS — {date_str}*",
        f"🗞️ {n_sources_ok}/{len(SOURCES)} fontes | {n_articles} manchetes",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔥 Viral (5+ fontes): {viral}",
        f"📈 Trending (3-4):    {trending}",
        f"📰 Múltiplas (2):     {multi}",
        f"🔍 Exclusiva (1):     {exclus}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    # Top 10 clusters
    for i, c in enumerate(clusters[:10], 1):
        _,em,n,has_inv,_=score_story(c)
        rep = pick_representative(c)
        title = rep.title[:55] + ("…" if len(rep.title)>55 else "")
        inv_flag = " 💡" if has_inv else ""
        lines.append(f"{em} *#{i}* {n}f{inv_flag} — {title}")

    if failed_sources:
        lines.append("━━━")
        lines.append(f"⚠️ Falhas: {', '.join(failed_sources)}")

    return "\n".join(lines)

def build_uncovered(solo_clusters, date_str):
    """Report stories covered by only 1 source — potential exclusives."""
    inv_solos    = [c for c in solo_clusters if c["sources"] & INVESTIGATIVE]
    grande_solos = [c for c in solo_clusters if c["sources"] <= GRANDE_IMPRENSA]

    lines = [f"🔍 *EXCLUSIVAS & INVESTIGATIVAS — {date_str}*",
             f"_{len(solo_clusters)} histórias em apenas 1 fonte_", "━━━"]

    if inv_solos:
        lines.append("*🔷 Investigativas (1 fonte):*")
        for c in inv_solos[:5]:
            rep = pick_representative(c)
            src = list(c["sources"])[0]
            emo = next((s["emoji"] for s in SOURCES if s["name"]==src), "")
            title = rep.title[:60] + ("…" if len(rep.title)>60 else "")
            link_part = f"[{src}]({rep.link})" if rep.link else src
            lines.append(f"  {emo} {link_part}: _{title}_")

    if grande_solos:
        lines.append("*📰 Grande imprensa (exclusivos):*")
        for c in grande_solos[:5]:
            rep = pick_representative(c)
            src = list(c["sources"])[0]
            emo = next((s["emoji"] for s in SOURCES if s["name"]==src), "")
            title = rep.title[:60] + ("…" if len(rep.title)>60 else "")
            link_part = f"[{src}]({rep.link})" if rep.link else src
            lines.append(f"  {emo} {link_part}: _{title}_")

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
                      "disable_web_page_preview": False, "disable_notification": silent},
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

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    hoje     = datetime.date.today()
    date_str = hoje.strftime("%d/%m/%Y")
    print(f"=== Monitor de Notícias v1.0 — {date_str} ===\n")

    # 1. Fetch all sources
    all_articles = []
    failed = []
    for source in SOURCES:
        arts = fetch_rss(source)
        if not arts:
            failed.append(source["name"])
        all_articles.extend(arts)

    total = len(all_articles)
    print(f"\n  Total: {total} artigos de {len(SOURCES)-len(failed)} fontes")

    if total < 5:
        send_telegram(f"⚠️ Monitor de Notícias {date_str}: apenas {total} artigos coletados. Verificar feeds.")
        return

    # 2. Cluster
    print("\n  Clusterizando...")
    clusters, recent = cluster_articles(all_articles, min_shared=2, window_hours=24)
    multi_source  = [c for c in clusters if len(c["sources"]) >= 2]
    single_source = [c for c in clusters if len(c["sources"]) == 1]
    print(f"  Clusters multi-fonte: {len(multi_source)}")
    print(f"  Histórias exclusivas: {len(single_source)}")

    # 3. Send summary
    summary = build_summary(multi_source + single_source[:5], recent, date_str, failed)
    send_telegram(summary)
    time.sleep(1)

    # 4. Send top trending story cards (most covered first)
    print("\n  Enviando fichas...")
    sent = 0
    for rank, cluster in enumerate(multi_source[:12], 1):
        _,_,n,_,_=score_story(cluster)
        if n < 2 and sent > 5: continue  # only include 2+ sources in detail
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

if __name__ == "__main__":
    main()
