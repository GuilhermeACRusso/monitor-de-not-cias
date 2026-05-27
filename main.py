
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
     "home": "https://g1.globo.com/"},
    {"name": "G1-SP",        "emoji": "🔵", "tier": "grande",
     "rss":  "https://g1.globo.com/dynamo/sao-paulo/rss2.xml",
     "home": "https://g1.globo.com/sp/sao-paulo/"},
    {"name": "Folha",        "emoji": "🟠", "tier": "grande",
     "rss":  "https://feeds.folha.uol.com.br/emcimadahora/rss091.xml",
     "home": "https://www1.folha.uol.com.br/ultimas-noticias/"},
    {"name": "Estadão",      "emoji": "🔴", "tier": "grande",
     "rss":  "https://www.estadao.com.br/arc/outboundfeeds/rss/",
     "home": "https://www.estadao.com.br/ultimas/"},
    {"name": "O Globo",      "emoji": "⚫", "tier": "grande",
     "rss":  "https://oglobo.globo.com/rss.xml",
     "home": "https://oglobo.globo.com/ultimas-noticias/"},
    {"name": "Metrópoles",   "emoji": "🟣", "tier": "grande",
     "rss":  "https://www.metropoles.com/feed/",
     "home": "https://www.metropoles.com/"},
    # Investigativa / especializada
    {"name": "Intercept",    "emoji": "🔷", "tier": "investigativa",
     "rss":  "https://theintercept.com/brasil/feed/?rss=1",
     "home": "https://www.intercept.com.br/"},
    {"name": "A Pública",    "emoji": "🟢", "tier": "investigativa",
     "rss":  "https://apublica.org/feed/",
     "home": "https://apublica.org/"},
    {"name": "Ag. Mural",    "emoji": "🟡", "tier": "investigativa",
     "rss":  "https://agenciamural.org.br/feed/",
     "home": "https://agenciamural.org.br/noticias/"},
    # Científica / acadêmica
    {"name": "Fiocruz",      "emoji": "🏥", "tier": "cientifica",
     "rss":  "https://agencia.fiocruz.br/feed",
     "home": "https://agencia.fiocruz.br/"},
    {"name": "Jornal USP",   "emoji": "🎓", "tier": "cientifica",
     "rss":  "https://jornal.usp.br/feed/",
     "home": "https://jornal.usp.br/"},
    {"name": "Ag. Galão",    "emoji": "🎭", "tier": "cultural",
     "rss":  "https://agenciagalo.com/feed/",
     "home": "https://agenciagalo.com/"},
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
    """Fetch and parse RSS feed. Returns list of Articles."""
    articles = []
    url = source.get("rss","")
    if not url: return articles
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"  {source['name']}: HTTP {r.status_code}")
            return articles
        body = r.content
        root = ET.fromstring(body)
        # Handle both RSS 2.0 and Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)
        for item in items:
            def get(tag, default=""):
                el = item.find(tag) or item.find(f"{{http://www.w3.org/2005/Atom}}{tag}")
                if el is None: return default
                return (el.text or default).strip()
            title = get("title") or get("headline")
            link  = get("link") or get("guid")
            # Atom uses <link href="..."/>
            if not link:
                lel = item.find("{http://www.w3.org/2005/Atom}link")
                if lel is not None: link = lel.get("href","")
            desc  = get("description") or get("summary") or get("content") or get("{http://purl.org/rss/1.0/modules/content/}encoded")
            date  = get("pubDate") or get("published") or get("updated")
            if title and len(title) > 10:
                articles.append(Article(title, link, desc, date, source["name"], source["emoji"]))
        print(f"  {source['name']}: {len(articles)} artigos ✅")
    except ET.ParseError:
        # Try HTML scraping fallback
        articles = fetch_html_headlines(source)
    except Exception as e:
        print(f"  {source['name']}: {e}")
    return articles

def fetch_html_headlines(source):
    """Fallback: scrape headlines from HTML page."""
    articles = []
    try:
        r = requests.get(source.get("home",""), headers=HEADERS, timeout=15)
        if r.status_code != 200: return articles
        html = r.text
        # Find headline links: <a href="...">Title text</a> or <h2>/<h3> content
        patterns = [
            r'<(?:h[123]|a)[^>]*href="(https?://[^"]+)"[^>]*>\s*([^<]{20,150})',
            r'"url"\s*:\s*"(https?://[^"]+)"[^}]*"headline"\s*:\s*"([^"]{20,150})"',
        ]
        seen = set()
        for pat in patterns:
            for href, text in re.findall(pat, html):
                text = clean_html(text).strip()
                if len(text) > 25 and text not in seen:
                    seen.add(text)
                    articles.append(Article(
                        text, href, "", "",
                        source["name"], source["emoji"]))
                    if len(articles) >= 30: break
        if articles:
            print(f"  {source['name']}: {len(articles)} artigos via HTML ✅")
    except Exception as e:
        print(f"  {source['name']} HTML fallback: {e}")
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
