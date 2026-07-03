"""
Monitor de Notícias v1.4 - Análise de cobertura jornalística brasileira
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

v1.1 changes (review of DOC-SP v9.8 methodology, applied where the
different architecture — cross-source clustering vs single-document
keyword extraction — makes them meaningful):

  BUG FIXES (found during this review):
  - civic_impact()/civic_action() category lookup was broken for 11 of
    13 categories due to accent mismatches (política vs politica) and
    label mismatches (São Paulo -> "paulo", not a real key). Replaced
    the split()[-1].lower() hack with an explicit label->key map.
  - Removed dead code in build_summary() (a no-op dict comprehension
    iterating over an empty list, computed and never used).
  - FOLLOWUP_RULES only ever suggested checking DOESP, never DOC-SP,
    even for stories clearly about city (not state) government. Added
    a municipal-government rule pointing to DOC-SP.

  NEW FEATURES (adapted from DOC-SP v9.8, attributed honestly):
  - VIP_WATCHLIST override — shared entity list with DOC-SP. A named
    figure/company bypasses is_relevant() filtering and verification-
    tier thresholds entirely.
  - News-value axis scoring (Magnitude/Deviance/Actionability), after
    Diakopoulos et al.'s news-value framework — NOT Spangher et al.
    (2024). Breaks ties within a verification tier and can promote a
    low-source-count but high-deviance story.
  - NewsLedger — persistent CSV tracking story fingerprints across
    days (Spangher et al. 2024: recurrence is a newsworthiness signal;
    News Monitor had NO persistent state before this version — every
    run was fully stateless, unlike DOC-SP's CNPJLedger since v9.6).
  - Negative case digest (Nowell et al. 2017) — weekly sample of
    articles that were filtered out by is_relevant() right at the
    threshold, for periodic human audit of the filter's calibration.
  - Selection-rate logging against Spangher et al.'s empirical
    benchmark (2-6% of source documents become "news" in their
    dataset; here applied as relevant-articles / recent-articles).
  - evaluate_ambiguous_story_via_llm() stub for future LLM-assisted
    curation of borderline single-source stories.

Secrets: TELEGRAM_TOKEN, CHAT_ID
Schedule: 4x/dia (7h, 12h, 17h, 20h BRT)
"""

import requests, datetime, os, sys, re, json, unicodedata, time, csv
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
if not TELEGRAM_TOKEN or not CHAT_ID:
    print("FATAL: TELEGRAM_TOKEN ou CHAT_ID ausentes."); sys.exit(1)

# ── VIP WATCHLIST (v1.1) ──────────────────────────────────────────
# Shared list with DOC-SP monitor — named entities under active public
# scrutiny. A match bypasses is_relevant() filtering and verification-
# tier thresholds; even a single-source mention gets surfaced.
VIP_WATCHLIST = [
    "PCC", "Transwolff", "Upbus", "Ricardo Nunes", "Milton Leite",
]

def check_vip_watchlist(text):
    """Returns the matched VIP term if found in the text, else None."""
    text_low = normalize(text)
    for term in VIP_WATCHLIST:
        if normalize(term) in text_low:
            return term
    return None


# ── NEWS LEDGER (v1.1) — persistent story tracking ─────────────────
# News Monitor had NO persistent state before this version: every run
# started from zero, so a story that has been building for three days
# looked identical to one that appeared for the first time an hour ago.
# Spangher et al. (2024) found that recurrence — an item being
# discussed/covered repeatedly — is itself a newsworthiness signal.
# This ledger tracks a lightweight "fingerprint" (top shared tokens)
# for each multi-source cluster across runs, so recurring stories can
# be flagged as ongoing rather than re-surfaced as if new each time.
class NewsLedger:
    FIELDNAMES = ["Data", "Fingerprint", "Headline", "Categoria", "NumFontes", "VIP"]

    def __init__(self, filename="news_ledger.csv", max_age_days=14):
        self.filename = filename
        self.records = []
        if os.path.exists(self.filename):
            try:
                with open(self.filename, mode='r', encoding='utf-8') as f:
                    raw = list(csv.DictReader(f))
                cutoff = datetime.date.today() - datetime.timedelta(days=max_age_days)
                kept = []
                for r in raw:
                    d = r.get("Data", "")
                    parts = d.split("/")
                    if len(parts) == 3:
                        try:
                            rd = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
                            if rd < cutoff:
                                continue
                        except ValueError:
                            pass
                    kept.append(r)
                self.records = kept
                if len(kept) < len(raw):
                    self._rewrite_csv()
                    print(f"📚 News ledger: {len(raw)}→{len(kept)} (podados {len(raw)-len(kept)})")
                else:
                    print(f"📚 News ledger: {len(kept)} registros carregados.")
            except Exception as e:
                print(f"🚨 Erro ao ler news_ledger.csv: {e}")

    def _rewrite_csv(self):
        try:
            with open(self.filename, mode='w', encoding='utf-8', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                w.writeheader()
                for r in self.records:
                    w.writerow(r)
        except Exception as e:
            print(f"🚨 Erro ao reescrever news_ledger.csv: {e}")

    @staticmethod
    def fingerprint(cluster, n=8):
        """
        Heuristic story identity: the top-N alphabetically-sorted shared
        tokens from the cluster. This is a coarse proxy for "same
        underlying story" across days, not a semantic match — it will
        miss stories that get re-described with different vocabulary
        and can occasionally over-merge unrelated stories that happen
        to share generic tokens. Documented limitation for v1.1;
        a v1.2 could use embedding similarity instead if this proves
        too noisy in practice.
        """
        toks = sorted(cluster.get("tokens", set()))[:n]
        return "|".join(toks)

    def check_recurrence(self, cluster, date_str, min_overlap=4, window_days=5):
        """
        Returns (days_seen, dates) if a sufficiently similar fingerprint
        (by token overlap) was logged on window_days prior distinct
        days, else None.
        """
        cutoff = datetime.date.today() - datetime.timedelta(days=window_days)
        current_tokens = set(cluster.get("tokens", set()))
        if not current_tokens:
            return None
        dates_seen = set()
        for r in self.records:
            d = r.get("Data", "")
            if d == date_str:
                continue
            parts = d.split("/")
            if len(parts) != 3:
                continue
            try:
                rd = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
            except ValueError:
                continue
            if rd < cutoff:
                continue
            past_tokens = set(r.get("Fingerprint", "").split("|"))
            if len(current_tokens & past_tokens) >= min_overlap:
                dates_seen.add(d)
        if dates_seen:
            return (len(dates_seen), sorted(dates_seen))
        return None

    def log_hit(self, date_str, cluster, cat_label, vip_term=None):
        fp = self.fingerprint(cluster)
        if not fp:
            return
        rep = pick_representative(cluster)
        row = {
            "Data": date_str,
            "Fingerprint": fp,
            "Headline": clean_headline(rep.title)[:120],
            "Categoria": cat_label,
            "NumFontes": str(len(cluster["sources"])),
            "VIP": vip_term or "",
        }
        self.records.append(row)
        try:
            with open(self.filename, mode='a', encoding='utf-8', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                if os.path.getsize(self.filename) == 0:
                    w.writeheader()
                w.writerow(row)
        except Exception as e:
            print(f"🚨 Erro ao salvar em news_ledger.csv: {e}")

    def weekly_summary(self):
        """v1.1: 7-day digest — story volume and repeat-coverage count."""
        cutoff = datetime.date.today() - datetime.timedelta(days=7)
        recent = []
        for r in self.records:
            parts = r.get("Data", "").split("/")
            if len(parts) == 3:
                try:
                    rd = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
                    if rd >= cutoff:
                        recent.append(r)
                except ValueError:
                    pass
        if not recent:
            return None
        dates = set(r.get("Data", "") for r in recent)
        cats = {}
        for r in recent:
            c = r.get("Categoria", "?")
            cats[c] = cats.get(c, 0) + 1
        top_cats = sorted(cats.items(), key=lambda x: -x[1])[:5]
        lines = [
            f"📈 *Resumo semanal (Monitor de Notícias)* ({len(dates)} dias, {len(recent)} pautas registradas)",
        ]
        if top_cats:
            lines.append("Categorias mais frequentes: " +
                          ", ".join(f"{c} ({n})" for c, n in top_cats))
        return "\n".join(lines)


# ===========================================================================
# ENTITY GRAPH — v1.2 (relationship network + timeline reconstruction)
# ===========================================================================
# Adapted from the DOC-SP monitor's EntityGraph (same three-table CSV
# schema, same class — see that codebase for the original design notes
# on Nowell et al. 2017 / Boyatzis 1998 / Spangher et al. 2024).
#
# Important difference from DOC-SP: News Monitor has no CNPJ, no formal
# "Empresa"/"Órgão"/"Servidor" fields extracted from structured
# government text — it works from free-text news prose. That means:
#   - Company/org entities are resolved by NAME MATCH ONLY, against a
#     curated KNOWN_ORGS list below, not by a stable ID like CNPJ. Two
#     different real-world entities that happen to share a name string
#     would incorrectly merge; a company referred to by very different
#     names across articles (no alias overlap) would incorrectly split.
#     This is a materially weaker identity-resolution guarantee than
#     DOC-SP's CNPJ-backed entities — flagged here rather than glossed
#     over, per Nowell et al.'s dependability/confirmability criteria
#     (be explicit about method limitations, don't just claim rigor).
#   - Person entities come from NAME_RE + noise filtering (the same
#     logic already used for the "who" field in extract_5w), which is
#     a heuristic capitalized-word-sequence matcher — it will miss
#     names in unusual formats and can occasionally catch non-names.
#   - "processo" (DOC-SP's SEI process number field) is repurposed here
#     to hold the representative article's URL — the closest analog
#     to an evidentiary reference for a news story.
import hashlib

class EntityGraph:
    ENTITY_FIELDS = ["entity_id", "entity_type", "canonical_name", "aliases",
                      "cnpj", "first_seen", "last_seen", "total_events"]
    REL_FIELDS = ["rel_id", "from_entity_id", "to_entity_id", "rel_type",
                  "first_seen", "last_seen", "evidence_count", "example_processo"]
    EVENT_FIELDS = ["event_id", "date", "entity_ids", "event_type", "category",
                     "description", "value", "processo", "source_doc_id", "keyword"]

    def __init__(self, base="news_graph", max_age_days=180):
        self.entities_path = f"{base}_entities.csv"
        self.relationships_path = f"{base}_relationships.csv"
        self.events_path = f"{base}_events.csv"
        self.entities = {}
        self.relationships = {}
        self.events = []
        self.max_age_days = max_age_days
        self._load()

    def _load(self):
        if os.path.exists(self.entities_path):
            try:
                with open(self.entities_path, encoding='utf-8') as f:
                    for row in csv.DictReader(f):
                        self.entities[row["entity_id"]] = row
            except Exception as e:
                print(f"🚨 Erro ao ler {self.entities_path}: {e}")
        if os.path.exists(self.relationships_path):
            try:
                with open(self.relationships_path, encoding='utf-8') as f:
                    for row in csv.DictReader(f):
                        key = (row["from_entity_id"], row["to_entity_id"], row["rel_type"])
                        self.relationships[key] = row
            except Exception as e:
                print(f"🚨 Erro ao ler {self.relationships_path}: {e}")
        if os.path.exists(self.events_path):
            try:
                with open(self.events_path, encoding='utf-8') as f:
                    raw = list(csv.DictReader(f))
                cutoff = datetime.date.today() - datetime.timedelta(days=self.max_age_days)
                kept = []
                for r in raw:
                    d = r.get("date", "")
                    parts = d.split("/")
                    if len(parts) == 3:
                        try:
                            rd = datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
                            if rd < cutoff:
                                continue
                        except ValueError:
                            pass
                    kept.append(r)
                self.events = kept
                if len(kept) < len(raw):
                    print(f"📚 Graph events: {len(raw)}→{len(kept)} (podados {len(raw)-len(kept)})")
            except Exception as e:
                print(f"🚨 Erro ao ler {self.events_path}: {e}")
        print(f"📊 Graph carregado: {len(self.entities)} entidades, "
              f"{len(self.relationships)} relações, {len(self.events)} eventos")

    def _save_entities(self):
        try:
            with open(self.entities_path, 'w', encoding='utf-8', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.ENTITY_FIELDS)
                w.writeheader()
                for row in self.entities.values():
                    w.writerow(row)
        except Exception as e:
            print(f"🚨 Erro ao salvar {self.entities_path}: {e}")

    def _save_relationships(self):
        try:
            with open(self.relationships_path, 'w', encoding='utf-8', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.REL_FIELDS)
                w.writeheader()
                for row in self.relationships.values():
                    w.writerow(row)
        except Exception as e:
            print(f"🚨 Erro ao salvar {self.relationships_path}: {e}")

    def _append_event(self, row):
        self.events.append(row)
        try:
            with open(self.events_path, 'a', encoding='utf-8', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.EVENT_FIELDS)
                if os.path.getsize(self.events_path) == 0:
                    w.writeheader()
                w.writerow(row)
        except Exception as e:
            print(f"🚨 Erro ao salvar evento: {e}")

    @staticmethod
    def _entity_id(entity_type, name, cnpj=None):
        if cnpj:
            key = re.sub(r'\D', '', cnpj)
            prefix = "EMP"
        else:
            key = normalize(name).strip()
            prefix = {"empresa": "EMP", "pessoa": "PES", "orgao": "ORG", "vip": "VIP"}.get(entity_type, "ENT")
        h = hashlib.md5(key.encode('utf-8')).hexdigest()[:10]
        return f"{prefix}-{h}"

    @staticmethod
    def _earlier(d1, d2):
        return d1 if _brdate_sortkey(d1) <= _brdate_sortkey(d2) else d2

    @staticmethod
    def _later(d1, d2):
        return d1 if _brdate_sortkey(d1) >= _brdate_sortkey(d2) else d2

    def get_or_create_entity(self, entity_type, name, date_str, cnpj=None):
        if not name:
            return None
        if entity_type == "vip":
            merge_id = self._find_mergeable_entity(name)
            if merge_id:
                row = self.entities[merge_id]
                row["first_seen"] = self._earlier(row["first_seen"], date_str)
                row["last_seen"] = self._later(row["last_seen"], date_str)
                row["total_events"] = str(int(row.get("total_events", "0") or "0") + 1)
                existing_aliases = set(a for a in row.get("aliases", "").split("|") if a)
                if normalize(name) != normalize(row["canonical_name"]) and name not in existing_aliases:
                    existing_aliases.add(name)
                    row["aliases"] = "|".join(sorted(existing_aliases))[:400]
                return merge_id

        eid = self._entity_id(entity_type, name, cnpj)
        if eid in self.entities:
            row = self.entities[eid]
            row["first_seen"] = self._earlier(row["first_seen"], date_str)
            row["last_seen"] = self._later(row["last_seen"], date_str)
            row["total_events"] = str(int(row.get("total_events", "0") or "0") + 1)
            existing_aliases = set(a for a in row.get("aliases", "").split("|") if a)
            if normalize(name) != normalize(row["canonical_name"]) and name not in existing_aliases:
                existing_aliases.add(name)
                row["aliases"] = "|".join(sorted(existing_aliases))[:400]
        else:
            row = {
                "entity_id": eid, "entity_type": entity_type,
                "canonical_name": name, "aliases": "",
                "cnpj": cnpj or "", "first_seen": date_str,
                "last_seen": date_str, "total_events": "1",
            }
            self.entities[eid] = row
        return eid

    def _find_mergeable_entity(self, vip_name):
        q = normalize(vip_name)
        if len(q) < 4:
            return None
        candidates = []
        for eid, row in self.entities.items():
            if row["entity_type"] == "vip":
                continue
            cname = normalize(row["canonical_name"])
            if q in cname or cname in q:
                candidates.append((len(cname), eid))
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][1]

    def add_relationship(self, from_id, to_id, rel_type, date_str, processo=""):
        if not from_id or not to_id or from_id == to_id:
            return
        key = (from_id, to_id, rel_type)
        if key in self.relationships:
            row = self.relationships[key]
            row["first_seen"] = self._earlier(row["first_seen"], date_str)
            row["last_seen"] = self._later(row["last_seen"], date_str)
            row["evidence_count"] = str(int(row.get("evidence_count", "0") or "0") + 1)
        else:
            row = {
                "rel_id": hashlib.md5(f"{from_id}|{to_id}|{rel_type}".encode()).hexdigest()[:10],
                "from_entity_id": from_id, "to_entity_id": to_id, "rel_type": rel_type,
                "first_seen": date_str, "last_seen": date_str,
                "evidence_count": "1", "example_processo": processo,
            }
            self.relationships[key] = row

    def add_event(self, date_str, entity_ids, event_type, category, description,
                  value="", processo="", source_doc_id="", keyword=""):
        entity_ids = [e for e in entity_ids if e]
        if not entity_ids:
            return
        event_id = hashlib.md5(
            f"{date_str}|{source_doc_id}|{keyword}|{'|'.join(entity_ids)}".encode()
        ).hexdigest()[:12]
        row = {
            "event_id": event_id, "date": date_str,
            "entity_ids": "|".join(entity_ids), "event_type": event_type,
            "category": category, "description": description[:200],
            "value": value, "processo": processo,
            "source_doc_id": source_doc_id, "keyword": keyword,
        }
        self._append_event(row)

    def flush(self):
        self._save_entities()
        self._save_relationships()

    def find_entity(self, query):
        q_digits = re.sub(r'\D', '', query)
        q_norm = normalize(query)
        if q_digits and len(q_digits) >= 11:
            for eid, row in self.entities.items():
                if re.sub(r'\D', '', row.get("cnpj", "")) == q_digits:
                    return eid
        for eid, row in self.entities.items():
            if q_norm in normalize(row["canonical_name"]):
                return eid
            if any(q_norm in normalize(a) for a in row.get("aliases", "").split("|") if a):
                return eid
        return None

    def get_timeline(self, query, limit=20):
        eid = self.find_entity(query)
        if not eid:
            return None, []
        matching = [e for e in self.events if eid in e.get("entity_ids", "").split("|")]
        matching.sort(key=lambda e: _brdate_sortkey(e.get("date", "")))
        return self.entities[eid], matching[-limit:]

    def get_network(self, query, depth=1):
        eid = self.find_entity(query)
        if not eid:
            return None, []
        edges = []
        for (fid, tid, rel_type), row in self.relationships.items():
            if fid == eid or tid == eid:
                other_id = tid if fid == eid else fid
                other = self.entities.get(other_id)
                if other:
                    edges.append((rel_type, other, row.get("evidence_count", "1")))
        edges.sort(key=lambda x: -int(x[2]))
        return self.entities[eid], edges

    def build_timeline_message(self, query, max_events=15):
        entity, events = self.get_timeline(query, limit=max_events)
        if not entity:
            return f"❓ Nenhuma entidade encontrada para '{query}'."
        lines = [
            f"🕓 *LINHA DO TEMPO — {entity['canonical_name']}*",
            f"_{entity['entity_type'].upper()} · primeiro registro {entity['first_seen']} · "
            f"{entity['total_events']} eventos totais_",
        ]
        if entity.get("aliases"):
            lines.append(f"_Também aparece como: {entity['aliases'][:150]}_")
        lines.append("─"*22)
        for e in events:
            val = f" · {e['value']}" if e.get("value") else ""
            lines.append(f"📅 *{e['date']}* — {e['event_type']}: {e['description']}{val}")
            if e.get("processo"):
                lines.append(f"   🔗 {e['processo']}")
        return "\n".join(lines)

    def build_network_message(self, query, max_edges=10):
        entity, edges = self.get_network(query)
        if not entity:
            return f"❓ Nenhuma entidade encontrada para '{query}'."
        lines = [
            f"🕸️ *REDE DE RELACIONAMENTOS — {entity['canonical_name']}*",
            f"_{len(edges)} conexão(ões) diretas_",
            "─"*22,
        ]
        for rel_type, other, evidence_count in edges[:max_edges]:
            lines.append(f"↔️ *{rel_type}* → {other['canonical_name']} "
                         f"({other['entity_type']}, {evidence_count}x)")
        return "\n".join(lines)


def _brdate_sortkey(d):
    parts = d.split("/")
    if len(parts) == 3:
        try:
            return (int(parts[2]), int(parts[1]), int(parts[0]))
        except ValueError:
            pass
    return (0, 0, 0)


# ── KNOWN ORGS (v1.2) — curated list for entity extraction ─────────
# News prose doesn't tag companies/agencies the way government gazette
# text does, so there's no reliable general-purpose extraction pattern.
# Instead we match cluster tokens against a curated list of
# organizations relevant to this beat: SP state/city agencies, state-
# owned companies, and companies that recur in Brazilian corruption/
# business coverage. This list needs periodic manual expansion as new
# names come up — it is deliberately not exhaustive (Boyatzis 1998:
# codes should be reviewed and expanded from what the real data shows,
# not guessed exhaustively up front).
KNOWN_ORGS = {
    "sabesp": "Sabesp", "cptm": "CPTM", "metro": "Metrô SP",
    "sptrans": "SPTrans", "emtu": "EMTU", "dersa": "Dersa",
    "prefeitura": "Prefeitura de São Paulo", "alesp": "Alesp",
    "detran": "Detran", "cetesb": "Cetesb", "cohab": "Cohab",
    "petrobras": "Petrobras", "bndes": "BNDES", "caixa": "Caixa Econômica Federal",
    "correios": "Correios", "eletrobras": "Eletrobras", "ibama": "Ibama",
    "anvisa": "Anvisa", "receita": "Receita Federal",
    "odebrecht": "Odebrecht/Novonor", "novonor": "Odebrecht/Novonor",
    "jbs": "JBS", "vale": "Vale", "americanas": "Americanas",
    "oi": "Oi", "vivo": "Vivo/Telefônica", "claro": "Claro",
    "ambev": "Ambev", "gerdau": "Gerdau", "embraer": "Embraer",
    "via": "Via (Casas Bahia)", "magalu": "Magazine Luiza",
}

def extract_org_entities(cluster):
    """Match cluster tokens against KNOWN_ORGS, return canonical names found."""
    tokens = cluster.get("tokens", set())
    found = []
    for token, canonical in KNOWN_ORGS.items():
        if token in tokens and canonical not in found:
            found.append(canonical)
    return found


def _record_cluster_to_graph(graph, date_str, cluster, category_key=""):
    """
    v1.2: translate one multi-source cluster into graph entities +
    relationships + a timeline event. Called only for clusters that
    already made it into the sent story cards (multi-source, or a
    single-source VIP hit) — same noise-avoidance principle as
    DOC-SP's equivalent function.
    """
    persons = []
    seen_norm = set()
    for art in cluster["articles"]:
        for name in extract_human_names(art.title + " " + art.description):
            if normalize(name) not in seen_norm:
                seen_norm.add(normalize(name))
                persons.append(name)

    orgs = extract_org_entities(cluster)

    vip_term = None
    for art in cluster["articles"]:
        vip_term = check_vip_watchlist(art.title + " " + art.description)
        if vip_term: break

    person_ids = [graph.get_or_create_entity("pessoa", p, date_str) for p in persons[:6]]
    org_ids = [graph.get_or_create_entity("empresa", o, date_str) for o in orgs]
    vip_id = graph.get_or_create_entity("vip", vip_term, date_str) if vip_term else None

    entity_ids = [e for e in (person_ids + org_ids + ([vip_id] if vip_id else [])) if e]
    if not entity_ids:
        return

    rep = pick_representative(cluster)
    cat_emoji, cat_label = story_category(cluster)
    w = extract_5w(cluster)
    event_type = normalize(cat_label).replace(" ", "_") or "noticia"

    graph.add_event(date_str, entity_ids, event_type, cat_label,
                    short_headline(rep.title, rep.description),
                    value=w.get("value", ""), processo=rep.link,
                    source_doc_id=rep.link[-40:] if rep.link else "", keyword=cat_label)

    for pid in person_ids:
        for oid in org_ids:
            graph.add_relationship(pid, oid, "associado_a", date_str, processo=rep.link)
    for i, pid1 in enumerate(person_ids):
        for pid2 in person_ids[i+1:]:
            graph.add_relationship(pid1, pid2, "citado_com", date_str, processo=rep.link)
    if vip_id:
        for oid in org_ids:
            graph.add_relationship(vip_id, oid, "mencionado_com", date_str, processo=rep.link)
        for pid in person_ids:
            graph.add_relationship(vip_id, pid, "mencionado_com", date_str, processo=rep.link)

# ── FONTES ────────────────────────────────────────────────────────
SOURCES = [
    # ── Grande imprensa (scope: nacional, except G1-SP which is local) ──
    {"name": "G1",           "emoji": "🔵", "tier": "grande", "scope": "nacional",
     "rss":  "https://g1.globo.com/dynamo/ultimas-noticias/rss2.xml",
     "home": "https://g1.globo.com/",
     "fmt":  "globo_json"},   # Globo uses JSON, not XML
    {"name": "G1-SP",        "emoji": "🔵", "tier": "grande", "scope": "local",
     "rss":  "https://g1.globo.com/dynamo/sao-paulo/rss2.xml",
     "home": "https://g1.globo.com/sp/sao-paulo/",
     "fmt":  "globo_json"},
    {"name": "Folha",        "emoji": "🟠", "tier": "grande", "scope": "nacional",
     "rss":  "https://feeds.folha.uol.com.br/emcimadahora/rss091.xml",
     "home": "https://www1.folha.uol.com.br/ultimas-noticias/",
     "fmt":  "rss"},
    {"name": "Estadão",      "emoji": "🔴", "tier": "grande", "scope": "nacional",
     "rss":  "https://www.estadao.com.br/arc/outboundfeeds/rss/?outputType=xml",
     "rss2": "https://www.estadao.com.br/ultimas/",
     "home": "https://www.estadao.com.br/ultimas/",
     "fmt":  "rss"},
    {"name": "O Globo",      "emoji": "⚫", "tier": "grande", "scope": "nacional",
     "rss":  "https://oglobo.globo.com/arc/outboundfeeds/rss/?outputType=xml",
     "rss2": "https://oglobo.globo.com/ultimas-noticias/",
     "home": "https://oglobo.globo.com/ultimas-noticias/",
     "fmt":  "rss"},
    {"name": "Metrópoles",   "emoji": "🟣", "tier": "grande", "scope": "nacional",
     "rss":  "https://www.metropoles.com/feed/",
     "home": "https://www.metropoles.com/",
     "fmt":  "rss"},
    # ── Investigativa / especializada (scope: nacional, except Ag. Mural = local) ──
    {"name": "Intercept",    "emoji": "🔷", "tier": "investigativa", "scope": "nacional",
     "rss":  "https://www.intercept.com.br/feed/",
     "home": "https://www.intercept.com.br/",
     "fmt":  "rss"},
    {"name": "A Pública",    "emoji": "🟢", "tier": "investigativa", "scope": "nacional",
     "rss":  "https://apublica.org/feed/",
     "home": "https://apublica.org/",
     "fmt":  "rss"},
    {"name": "Ag. Mural",    "emoji": "🟡", "tier": "investigativa", "scope": "local",
     "rss":  "https://agenciamural.org.br/wp-json/wp/v2/posts?per_page=25&orderby=date&order=desc&_fields=title,link,excerpt,date",
     "rss2": "https://agenciamural.org.br/feed/",
     "rss3": "https://agenciamural.org.br/noticias/",
     "home": "https://agenciamural.org.br/noticias/",
     "fmt":  "wp_api"},
    # ── Científica / acadêmica (scope: nacional, except Jornal USP = local) ──
    {"name": "Fiocruz",      "emoji": "🏥", "tier": "cientifica", "scope": "nacional",
     "rss":  "https://agencia.fiocruz.br/feed/",
     "rss2": "https://agencia.fiocruz.br/wp-json/wp/v2/posts?per_page=25&orderby=date&order=desc&_fields=title,link,excerpt,date",
     "rss3": "https://agencia.fiocruz.br/noticias",
     "home": "https://agencia.fiocruz.br/",
     "fmt":  "rss"},
    {"name": "Jornal USP",   "emoji": "🎓", "tier": "cientifica", "scope": "local",
     "rss":  "https://jornal.usp.br/feed/",
     "home": "https://jornal.usp.br/",
     "fmt":  "rss"},
    {"name": "Ag. Galão",    "emoji": "🎭", "tier": "cultural", "scope": "nacional",
     "rss":  "https://agenciagalo.com/wp-json/wp/v2/posts?per_page=25&orderby=date&order=desc&_fields=title,link,excerpt,date",
     "rss2": "https://agenciagalo.com/feed/",
     "rss3": "https://agenciagalo.com/",
     "home": "https://agenciagalo.com/",
     "fmt":  "wp_api"},

    # ══════════════════════════════════════════════════════════════
    # v1.3: LOCAL — São Paulo capital neighborhoods (scope: local)
    # ══════════════════════════════════════════════════════════════
    # These are small, independently-run regional outlets. RSS URLs
    # below are best-effort (the standard WordPress /feed/ pattern,
    # since all four sites run on WordPress/Elementor per their page
    # markup) — NOT individually verified against live feed output.
    # The existing fetch_rss() architecture already tries multiple URL
    # candidates then falls back to Playwright HTML scraping of the
    # "home" page if RSS fails, so a wrong guess degrades gracefully
    # rather than silently losing the source.
    {"name": "ZL Notícias",   "emoji": "🌇", "tier": "local", "scope": "local",
     "rss":  "https://zlnoticias.com.br/feed/",
     "home": "https://zlnoticias.com.br/",
     "fmt":  "rss"},   # Zona Leste — ~4M habitantes
    {"name": "SP Zona Sul",   "emoji": "🌆", "tier": "local", "scope": "local",
     "rss":  "https://jornalzonasul.com.br/feed/",
     "home": "https://jornalzonasul.com.br/",
     "fmt":  "rss"},   # Zona Sul
    {"name": "Notícias de Itaquera", "emoji": "🏙️", "tier": "local", "scope": "local",
     "rss":  "https://www.noticiasdeitaquera.com.br/feed/",
     "home": "https://www.noticiasdeitaquera.com.br/noticias/",
     "fmt":  "rss"},   # Itaquera e região (Zona Leste)
    {"name": "Infoleste",     "emoji": "🌉", "tier": "local", "scope": "local",
     "rss":  "https://infoleste.com.br/feed/",
     "home": "https://infoleste.com.br/",
     "fmt":  "rss"},   # Zona Leste — imprensa comunitária

    # ══════════════════════════════════════════════════════════════
    # v1.3: LOCAL — Região Metropolitana de São Paulo (Grande SP)
    # ══════════════════════════════════════════════════════════════
    # Grouped under the same "local" scope as SP-capital neighborhoods
    # per your explicit framing: "local news sites that refer to
    # neighborhoods in São Paulo, AND the cities in the metropolitan
    # area" — both are "closest to me" for a São Paulo capital-based
    # journalist, as distinct from "estadual" (rest of SP state).
    {"name": "Diário do Grande ABC", "emoji": "🏭", "tier": "local", "scope": "local",
     "rss":  "https://www.dgabc.com.br/rss",
     "rss2": "https://www.dgabc.com.br/Rss/UltimasNoticias",
     "home": "https://www.dgabc.com.br/",
     "fmt":  "rss"},   # Santo André, São Bernardo, São Caetano, Diadema, Mauá,
                        # Ribeirão Pires, Rio Grande da Serra — maior jornal regional do país
    {"name": "Repórter Diário", "emoji": "🏗️", "tier": "local", "scope": "local",
     "rss":  "https://www.reporterdiario.com.br/feed/",
     "home": "https://www.reporterdiario.com.br/",
     "fmt":  "rss"},   # ABC — segunda fonte para checagem cruzada
    {"name": "Guarulhos Hoje", "emoji": "✈️", "tier": "local", "scope": "local",
     "rss":  "https://www.guarulhoshoje.com.br/feed/",
     "home": "https://www.guarulhoshoje.com.br/",
     "fmt":  "rss"},   # Guarulhos (2ª maior cidade da Grande SP)
    {"name": "Folha Metropolitana", "emoji": "🚇", "tier": "local", "scope": "local",
     "rss":  "https://www.fmetropolitana.com.br/feed/",
     "home": "https://www.fmetropolitana.com.br/",
     "fmt":  "rss"},   # Guarulhos — segunda fonte

    # ══════════════════════════════════════════════════════════════
    # v1.3: INTERNACIONAL — wire services and major outlets by region
    # ══════════════════════════════════════════════════════════════
    # Selected for (a) major economic/political ties to Brazil, and
    # (b) general international relevance, spanning continents. Most
    # URLs below are independently verified against live RSS output
    # (see chat for research notes); CGTN and Infobae are best-effort
    # (site loads via JS-heavy delivery, relies on Playwright fallback).
    {"name": "BBC World",     "emoji": "🇬🇧", "tier": "internacional", "scope": "internacional",
     "rss":  "https://feeds.bbci.co.uk/news/world/rss.xml",
     "home": "https://www.bbc.com/news/world",
     "fmt":  "rss"},
    {"name": "BBC América Latina", "emoji": "🌎", "tier": "internacional", "scope": "internacional",
     "rss":  "https://feeds.bbci.co.uk/news/world/latin_america/rss.xml",
     "home": "https://www.bbc.com/news/world/latin_america",
     "fmt":  "rss"},
    {"name": "BBC África",    "emoji": "🌍", "tier": "internacional", "scope": "internacional",
     "rss":  "https://feeds.bbci.co.uk/news/world/africa/rss.xml",
     "home": "https://www.bbc.com/news/world/africa",
     "fmt":  "rss"},
    {"name": "BBC Ásia",      "emoji": "🌏", "tier": "internacional", "scope": "internacional",
     "rss":  "https://feeds.bbci.co.uk/news/world/asia/rss.xml",
     "home": "https://www.bbc.com/news/world/asia",
     "fmt":  "rss"},
    {"name": "Al Jazeera",    "emoji": "🕌", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.aljazeera.com/xml/rss/all.xml",
     "home": "https://www.aljazeera.com/",
     "fmt":  "rss"},   # Oriente Médio + cobertura global
    {"name": "DW",            "emoji": "🇩🇪", "tier": "internacional", "scope": "internacional",
     "rss":  "https://rss.dw.com/rdf/rss-en-top",
     "home": "https://www.dw.com/en/top-stories/s-9097",
     "fmt":  "rss"},   # Alemanha/Europa — maior economia da UE
    {"name": "France24",      "emoji": "🇫🇷", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.france24.com/en/rss",
     "home": "https://www.france24.com/en/",
     "fmt":  "rss"},
    {"name": "CGTN",          "emoji": "🇨🇳", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.cgtn.com/subscribe/rss/world.xml",
     "home": "https://www.cgtn.com/world",
     "fmt":  "rss"},   # China — maior parceiro comercial do Brasil
    {"name": "Infobae",       "emoji": "🇦🇷", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.infobae.com/arc/outboundfeeds/rss/",
     "home": "https://www.infobae.com/america/",
     "fmt":  "rss"},   # Argentina — Mercosul, vizinho mais próximo
    {"name": "ABC News AU",   "emoji": "🇦🇺", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.abc.net.au/news/feed/51120/rss.xml",
     "home": "https://www.abc.net.au/news/world",
     "fmt":  "rss"},   # Austrália — cobertura Oceania
    {"name": "UPI World",     "emoji": "🌐", "tier": "internacional", "scope": "internacional",
     "rss":  "https://rss.upi.com/news/tn_int.rss",
     "home": "https://www.upi.com/Top_News/World-News/",
     "fmt":  "rss"},   # agência de notícias EUA — cobertura global

    # ══════════════════════════════════════════════════════════════
    # v1.4: BRASIL — mídia alternativa (scope: nacional)
    # ══════════════════════════════════════════════════════════════
    # Requested by name: Opera Mundi and Brasil de Fato, both real,
    # active, editorially independent from the "grande imprensa"
    # conglomerates already in this list (Globo, Folha/UOL group,
    # Estadão). Their content will often get classified as
    # "internacional" by geo_scope() at the per-story level (correct —
    # a Brazilian outlet covering Gaza should show in that bucket),
    # even though the source itself is Brazilian, hence scope=nacional.
    {"name": "Opera Mundi",   "emoji": "🌐", "tier": "alternativa", "scope": "nacional",
     "rss":  "https://operamundi.uol.com.br/feed",
     "home": "https://operamundi.uol.com.br/",
     "fmt":  "rss"},   # fundado 2008, cobertura internacional, editorial
                        # independente e progressista, sem vínculo com
                        # grandes grupos econômicos
    {"name": "Brasil de Fato", "emoji": "✊", "tier": "alternativa", "scope": "nacional",
     "rss":  "https://www.brasildefato.com.br/rss2.xml",
     "rss2": "https://www.brasildefato.com.br/feed",
     "home": "https://www.brasildefato.com.br/",
     "fmt":  "rss"},   # fundado por movimentos populares em 2003 durante
                        # o Fórum Social Mundial em Porto Alegre

    # ══════════════════════════════════════════════════════════════
    # v1.4: AMÉRICA LATINA — rede de jornalismo investigativo
    # independente (scope: internacional)
    # ══════════════════════════════════════════════════════════════
    # These outlets are part of a documented, real network of
    # independent Latin American investigative journalism (cited
    # together in ICIJ's Panama/Paradise Papers media-partner lists and
    # in academic literature on regional media capture) — deliberately
    # not the "hegemonic" corporate press (Clarín, Televisa, O Globo-
    # style conglomerates). Several operate under real political
    # pressure: El Faro relocated its newsroom from El Salvador to
    # Costa Rica in 2023 due to state persecution.
    {"name": "El Faro",       "emoji": "🇸🇻", "tier": "internacional", "scope": "internacional",
     "rss":  "https://elfaro.net/en/rss",
     "home": "https://elfaro.net/en",
     "fmt":  "rss"},   # El Salvador — investigativo, relocado para Costa
                        # Rica em 2023 por perseguição do governo Bukele
    {"name": "CIPER Chile",   "emoji": "🇨🇱", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.ciperchile.cl/feed/",
     "home": "https://www.ciperchile.cl/",
     "fmt":  "rss"},   # Chile — jornalismo investigativo sem fins lucrativos
    {"name": "Ojo Público",   "emoji": "🇵🇪", "tier": "internacional", "scope": "internacional",
     "rss":  "https://ojo-publico.com/feed",
     "home": "https://ojo-publico.com/",
     "fmt":  "rss"},   # Peru — investigativo, dados abertos

    # ══════════════════════════════════════════════════════════════
    # v1.4: ESTADOS UNIDOS — imprensa alternativa/independente
    # (scope: internacional)
    # ══════════════════════════════════════════════════════════════
    # Explicitly non-corporate US press, as distinct from the
    # hegemonic broadcast/cable landscape (CNN/Fox/NYT-style). "The
    # Intercept" here is the US edition (theintercept.com) — a
    # different outlet/domain from "Intercept" (intercept.com.br,
    # Brazilian edition) already in this list.
    {"name": "ProPublica",    "emoji": "🇺🇸", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.propublica.org/feeds/propublica/main",
     "home": "https://www.propublica.org/",
     "fmt":  "rss"},   # EUA — jornalismo investigativo sem fins lucrativos,
                        # fundado 2007
    {"name": "The Intercept (US)", "emoji": "🗽", "tier": "internacional", "scope": "internacional",
     "rss":  "https://theintercept.com/feed/",
     "home": "https://theintercept.com/",
     "fmt":  "rss"},   # EUA — vigilância, segurança nacional, política —
                        # edição distinta do Intercept Brasil
    {"name": "Democracy Now!", "emoji": "📢", "tier": "internacional", "scope": "internacional",
     "rss":  "https://www.democracynow.org/democracynow.rss",
     "home": "https://www.democracynow.org/",
     "fmt":  "rss"},   # EUA — noticiário diário independente, Amy Goodman
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
    # v1.3: bairros e zonas da capital paulista — extends the SAME "São Paulo"
    # geographic category above, not a new editorial category. Needed so
    # purely local neighborhood news (which often has no "politica"/"economia"
    # token) still passes the topic filter. Single-word tokens only — this
    # set is checked via token-set intersection (see tokenize()), which
    # extracts individual words, so multi-word names like "Santo André"
    # would never match here and are instead handled by substring checks
    # in geo_scope() below.
    "pinheiros","moema","tatuape","itaquera","santana","mooca","butanta","lapa",
    "ipiranga","liberdade","penha","sacoma","jabaquara","brasilandia","perus",
    "cambuci","consolacao","aricanduva","sapopemba","guaianases","ermelino",
    "freguesia","tucuruvi","periferia","periferias",
    # v1.3: cidades da Região Metropolitana de São Paulo (Grande SP) —
    # single-word city names only; multi-word ones (Santo André, São
    # Bernardo, etc.) are matched by substring in geo_scope() instead.
    "guarulhos","osasco","diadema","maua","barueri","cotia","carapicuiba",
    "suzano","itapevi","aruja","poa","itaquaquecetuba",
    # v1.3: cidades do interior/litoral paulista (single-word only; see
    # geo_scope() for multi-word interior city matching)
    "campinas","santos","sorocaba","bauru","piracicaba","jundiai","franca",
    "marilia","araraquara",
    # Governo federal
    "lula","planalto","stf","supremo","tcm","tcu","tce","ministerio","federal",
    # v1.3: política internacional — extends the existing international-
    # politics interest (already named as a category to keep), covers
    # conflict/diplomacy vocabulary that pure "politica"/"economia" tokens
    # don't reliably catch (e.g. war coverage, treaties, summits).
    "guerra","conflito","invasao","sancao","cupula","tratado","diplomacia",
    "embaixada","onu","otan","brics","geopolitica","refugiado","fronteira",
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

# Sports tokens — any 1+ triggers the sports check
_SPORTS_TOKENS = {
    # Sports & competitions
    "futebol","basquete","volei","natacao","atletismo","ciclismo","tenis",
    "gol","placar","rodada","tabela","campeonato","copa","olimpiada",
    "olimpico","olimpicos","paralimpico","semifinal","quartas","titulo",
    "final","torneio","partida","torcida","estadio","arena","liga","esporte",
    # Athletes / teams
    "jogador","atleta","tecnico","treinador","escalacao","elenco",
    "transferencia","renovacao",  # player-specific contracts/transfers
    # Clubs (most common Brazilian ones)
    "palmeiras","flamengo","corinthians","santos","atletico","gremio",
    "fluminense","vasco","botafogo","internacional","cruzeiro","bahia",
    "sao paulo","sport","celta","galvao","neymar","mbappé","mbappe",
}

# Institutional signals that OVERRIDE the sports block
# If any of these appear alongside sports → keep (political sports angle)
_INSTITUTIONAL_SPORTS = {
    "corrupcao","propina","lavagem","fraude","desvio","superfaturamento",
    "licitacao","verba","publica","erario","fundo","repasse","recurso",
    "cpi","cpis","tcu","ministerio","senado","camara","governador","prefeito",
    "governadora","prefeitura","deputado","deputados","senador","senadores",
    "policia","federal","ministerio","publico","delegado","investigacao",
    "apuracao","prisao","preso","presa","condenado","condenados",
    "stf","stj","pgr","regulacao","legislacao","regulamentacao",
    "cbf","conmebol","anfp","cob","esporte","antidoping","doping",
    "fraude","escandalo","corrupto",
    # Betting/gambling manipulation — always political/criminal context
    "manipulacao","manipulado","viciado","fixacao","combinado",
    "apostas","bicheiro","jogo ilegal","lavagem",
}

# Also check title text directly for 3-char acronyms that regex misses
_SHORT_INST_SIGNALS = {"cpi","tcu","stf","stj","pgr","pf","mpf","cvm","bcb",
                       "cob","cbf","can","van","onu"}


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

# v1.3/v1.4: sources exempted from the "must have a Brazil angle"
# requirement in is_relevant() below — same principle as the
# pre-existing investigative-outlet exemption, extended to the
# international wire sources (v1.3) and to Opera Mundi/Brasil de Fato
# (v1.4), whose entire editorial purpose is covering stories that
# legitimately have no Brazil connection (e.g. "China's exports grow
# 5%", "settlers in the West Bank"). Both are scope="nacional" (they're
# Brazilian outlets) so they don't get auto-included by the scope
# check below — added explicitly here instead.
_ALWAYS_RELEVANT_SOURCES = {
    "Intercept", "A Pública", "Ag. Mural", "Fiocruz",
    "Opera Mundi", "Brasil de Fato",
} | {
    s["name"] for s in SOURCES if s.get("scope") == "internacional"
}

# ── GEOGRAPHIC SCOPE (v1.3) ─────────────────────────────────────────
# Purely about WHERE a story is set — local / estadual / nacional /
# internacional — orthogonal to story_category() (WHAT it's about).
# Uses substring matching on the full normalized text rather than
# token-set intersection, because most place names here are multi-word
# ("Santo André", "São Bernardo do Campo") and tokenize()'s single-word
# extraction would never match them via set intersection (the same
# pattern issue noted elsewhere in this file, e.g. FOLLOWUP_RULES).
_LOCAL_SP_PLACES = [
    # Bairros/zonas da capital (single AND multi-word forms)
    "pinheiros","moema","tatuape","itaquera","santana","mooca","butanta",
    "lapa","ipiranga","liberdade","penha","sacoma","jabaquara","brasilandia",
    "perus","cambuci","consolacao","aricanduva","sapopemba","guaianases",
    "ermelino","freguesia do o","tucuruvi","vila mariana","vila prudente",
    "vila formosa","vila matilde","são miguel paulista","cidade tiradentes",
    "itaim paulista","zona leste","zona sul","zona norte","zona oeste",
    "centro de são paulo","subprefeitura",
    # Região Metropolitana (Grande SP) — single AND multi-word city names
    "guarulhos","osasco","diadema","maua","barueri","cotia","carapicuiba",
    "suzano","itapevi","aruja","itaquaquecetuba","santo andre","sao bernardo",
    "sao caetano","taboao da serra","mogi das cruzes","embu das artes",
    "franco da rocha","itapecerica da serra","ribeirao pires",
    "rio grande da serra","ferraz de vasconcelos","grande abc","abc paulista",
    "grande sao paulo","regiao metropolitana",
]
_ESTADUAL_SP_PLACES = [
    # Interior/litoral paulista — outside the metro region
    "campinas","santos","sorocaba","bauru","piracicaba","jundiai","franca",
    "marilia","araraquara","ribeirao preto","sao jose dos campos",
    "sao jose do rio preto","presidente prudente","litoral paulista",
    "baixada santista","vale do paraiba","interior paulista","interior de sao paulo",
    "tarcisio","alesp","governo do estado","governo estadual","secretaria estadual",
]

# v1.4: Brazil-SPECIFIC signal for geo_scope's internacional/nacional
# split — deliberately a stricter subset of _BRAZIL_TOKENS. Found via
# testing: _BRAZIL_TOKENS includes generic institutional words like
# "governo", "presidente", "ministro" that apply to ANY country's
# government, not just Brazil's — an El Faro story about "governo de
# El Salvador" was incorrectly classified as "nacional" because
# "governo" alone tripped has_brazil. is_relevant() still uses the
# original _BRAZIL_TOKENS unchanged (that filter's calibration is a
# separate concern); this stricter set is only for geo_scope's binary
# "is this genuinely about Brazil" question.
_BRAZIL_SPECIFIC_TOKENS = {
    "brasil","brasileiro","brasileira","brasileiros","brasileiras",
    "paulo","paulista","carioca","brasilia","mineiro","gaucho",
    "baiano","cearense","pernambucano","fluminense","minas",
    "lula","bolsonaro","tarcisio","haddad","moro","flavio","eduardo",
    "gleisi","tebet","pacheco","lira","ciro","marina","damares",
    "stf","stj","tcu","tse","pgr","mpf","ibge","ibama","funai","inpe",
    "planalto",
    "real","reais","brl","selic","ipca","ibovespa",
    "pt","pl","mdb","psdb","psd","republicanos","solidariedade",
    "feminicidio","femicidio",
}

def geo_scope(cluster):
    """
    v1.3: classify a cluster by WHERE the story is set, for message
    grouping (closest to farthest from a São Paulo capital-based
    journalist): local -> estadual -> nacional -> internacional.
    Purely geographic — does not affect story_category() or any
    editorial filtering in is_relevant().
    """
    text_low = normalize(" ".join(a.title + " " + a.description for a in cluster["articles"]))
    tokens = cluster.get("tokens", set())

    if any(place in text_low for place in _LOCAL_SP_PLACES):
        return "local"
    if any(place in text_low for place in _ESTADUAL_SP_PLACES):
        return "estadual"
    # v1.4: use the stricter Brazil-specific set here, not the full
    # _BRAZIL_TOKENS (which includes generic words like "governo" that
    # would misclassify e.g. an El Salvador government story).
    has_brazil = bool(tokens & _BRAZIL_SPECIFIC_TOKENS)
    if not has_brazil:
        return "internacional"
    return "nacional"


_GEO_LABELS = {
    "local":         ("📍", "LOCAL — São Paulo e Região Metropolitana"),
    "estadual":      ("🗺️", "ESTADUAL — Interior e Litoral de São Paulo"),
    "nacional":      ("🇧🇷", "NACIONAL — Brasil"),
    "internacional": ("🌍", "INTERNACIONAL — Mundo"),
}

def is_relevant(article):
    """Return True if the article matches monitored topics and isn't in blocked categories."""
    title_low = normalize(article.title)
    desc_low  = normalize(article.description)
    combined  = title_low + " " + desc_low

    # v1.1: VIP watchlist bypass — a named entity under scrutiny is
    # always relevant, regardless of sports/celebrity/international
    # filtering below.
    if check_vip_watchlist(combined):
        return True

    tokens = set(re.findall(r"[a-zà-ÿ]{4,}", combined))

    # Hard-block phrases (always reject regardless of other signals)
    for phrase in _BLOCK_HARD_PHRASES:
        if normalize(phrase) in combined:
            return False
    for phrase in _BLOCK_PHRASES:
        if normalize(phrase) in combined:
            return False

    blocked_hits      = len(tokens & _BLOCK_TOKENS)
    keep_hits         = len(tokens & _KEEP_TOKENS)
    sports_hits       = len(tokens & _SPORTS_TOKENS)
    institutional_hits= len(tokens & _INSTITUTIONAL_SPORTS)

    # Sports filter — block unless institutional/political angle present
    has_brazil = bool(tokens & _BRAZIL_TOKENS)
    if sports_hits >= 1 and institutional_hits == 0:
        # Check 3-char acronyms regex misses (cpi, tcu, pf, etc.)
        words_3 = set(re.findall(r"\b[a-z]{2,3}\b", title_low + " " + desc_low))
        if words_3 & _SHORT_INST_SIGNALS:
            institutional_hits = 1
        # Check political figure in title (Tarcísio/Lula + stadium/sport = keep)
        elif any(t in title_low for t in
                 ["tarcisio","lula","haddad","governador","prefeito","ministerio","governo"]):
            institutional_hits = 1
        else:
            return False  # pure sports — no political/institutional angle

    # Celebrity entertainment: block
    if blocked_hits >= 2 and keep_hits == 0:
        return False

    # International stories must have a Brazilian angle — UNLESS the
    # source is one we exempt (investigative outlets + v1.3 international
    # wire sources, whose whole purpose is non-Brazil content).
    if keep_hits == 0 and institutional_hits == 0 and not has_brazil \
       and article.source not in _ALWAYS_RELEVANT_SOURCES:
        return False

    if keep_hits <= 1 and institutional_hits == 0 and not has_brazil \
       and article.source not in _ALWAYS_RELEVANT_SOURCES:
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




def _parse_tcu_json(text, name, emoji):
    """Parse TCU acórdãos webservice JSON response."""
    articles = []
    try:
        data = json.loads(text)
        # Webservice returns {total, results: [{numeroAcordao, colegiado, dataSessao,
        #                                       relator, sumario, urlAcordao}]}
        items = data.get("results") or data.get("hits") or []
        if isinstance(data, list): items = data
        for item in items[:20]:
            n    = item.get("numeroAcordao") or item.get("numeroAcordao","")
            col  = item.get("colegiado","")
            date = item.get("dataSessao") or item.get("dataSessionAnterior","")
            rel  = item.get("relator","")
            summ = item.get("sumario") or item.get("ementa","")
            url  = item.get("urlAcordao") or item.get("url","")
            if n:
                title = f"Acórdão {n}/{_year_from_date(date)} – {col}"
                desc  = f"Relator: {rel}. {clean_html(summ)}"[:400] if summ else ""
                articles.append(Article(title, url, desc, date, name, emoji))
    except (json.JSONDecodeError, KeyError, TypeError): pass
    return articles

def _year_from_date(d):
    import re as _re
    m = _re.search(r"\d{4}", str(d))
    return m.group(0) if m else "2026"

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
    r"(Times\s+Square|Boulevard|Departamento\s+de|Ministério\s+da|"
    r"Ministério\s+Público|Ministério\s+do|"
    r"Supremo\s+Tribunal|Superior\s+Tribunal|Polícia\s+Civil|"
    r"Polícia\s+Federal|Prefeitura|Secretaria|Câmara\s+dos|"
    r"Congresso\s+Nacional|São\s+Paulo|Rio\s+de\s+Janeiro|"
    r"Belo\s+Horizonte|Porto\s+Alegre)", re.I)

def extract_human_names(text, limit=None):
    """
    v1.2: factored out of extract_5w() so it can be reused by the entity
    graph (which needs ALL names found across a cluster, not just the
    top 2 for the "who" field of the representative article).
    Same filtering logic as before: skip known noise patterns, skip
    all-caps (usually acronyms), skip names containing a known place
    word, skip anything under 4 chars.
    """
    raw_names = list(dict.fromkeys(NAME_RE.findall(text)))
    human_names = []
    for n in raw_names:
        if _NOISE_NAMES.search(n): continue
        if n.isupper(): continue
        words = normalize(n).split()
        if any(w in PLACE_NAMES for w in words): continue
        if len(n) < 4: continue
        human_names.append(n)
    return human_names[:limit] if limit else human_names


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
    human_names = extract_human_names(full_text, limit=2)
    who = ", ".join(human_names) if human_names else ""

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

# Source type groups for independence analysis (Kovach Ch.5)
MAINSTREAM    = {"G1","G1-SP","Folha","Estadão","O Globo","Metrópoles"}
INVESTIGATIVE_PLUS = INVESTIGATIVE | {"Intercept","A Pública","Ag. Mural"}

def score_story(cluster):
    """
    Kovach Ch.2+4: Truth levels — verification depends on source count AND diversity.
    Tiers: VERIFICADO (multiple independent witnesses) > RELATADO > APURADO > AVISO
    """
    sources = cluster["sources"]
    n       = len(sources)
    inv     = sources & INVESTIGATIVE_PLUS
    grande  = sources & MAINSTREAM
    both    = bool(inv) and bool(grande)  # cross-verified

    # Verification tiers (replaces raw viral/trending labels)
    if n >= 4 or (n >= 3 and both):
        label, emoji = "🔒 VERIFICADO", "🔒"
    elif n >= 3:
        label, emoji = "📋 MÚLTIPLAS", "📋"
    elif n == 2 and both:
        label, emoji = "📋 CRUZADO", "📋"  # 2 sources from different groups
    elif n == 2:
        label, emoji = "📰 RELATADO", "📰"
    elif sources <= INVESTIGATIVE_PLUS:
        label, emoji = "💡 APURADO", "💡"   # single investigative = deeper reporting
    else:
        label, emoji = "📡 AVISO", "📡"     # single mainstream = least verified
    return label, emoji, n, bool(inv), bool(grande)


# ── NEWS-VALUE AXIS SCORING (v1.1) ─────────────────────────────────
# Inspired by Diakopoulos et al.'s news-value framework (novelty,
# magnitude, organizational agenda) used in the Algorithm Tips
# government-document monitor — NOT Spangher et al. (2024), which uses
# a different methodology (PRM chains + empirical word-deltas).
#
# score_story() above measures verification (HOW MANY sources agree),
# which is orthogonal to newsworthiness (HOW MUCH this matters). A
# story with 5 sources repeating a press release and a story with 1
# investigative source breaking a corruption lead get the same
# verification tier treatment otherwise. These axes let us break ties
# within a tier and flag high-deviance stories even at low source count.
_DEVIANCE_TOKENS = {
    "corrupcao","fraude","desvio","superfaturamento","lavagem","escandalo",
    "prisao","preso","condenado","investigacao","emergencial","urgente",
    "improbidade","propina","suborno","vazamento","denuncia",
}

def compute_news_axes(cluster):
    """
    Magnitude (M):     a specific large monetary value is mentioned
    Deviance (D):      departure from routine — crime/corruption/
                        emergency language present in cluster tokens
    Actionability (A):  a specific named person AND a specific place
                        are both identifiable — gives a reporter
                        something concrete to pursue
    """
    tokens = cluster.get("tokens", set())
    text = " ".join(a.title + " " + a.description for a in cluster["articles"])

    magnitude = bool(VALUE_RE.search(text))
    deviance = bool(tokens & _DEVIANCE_TOKENS)

    has_name = bool(NAME_RE.search(text))
    has_place = bool(PLACE_RE.search(text))
    actionability = has_name and has_place

    return {"magnitude": magnitude, "deviance": deviance, "actionability": actionability}


def axis_badge(axes):
    """v1.1: compact badge string for story cards, e.g. 'M·D'."""
    parts = []
    if axes.get("magnitude"): parts.append("💰M")
    if axes.get("deviance"): parts.append("🚨D")
    if axes.get("actionability"): parts.append("🎯A")
    return " ".join(parts)

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
    # v1.1: SP city govt → check DOC-SP (was previously missing — every
    # rule pointed to DOESP even for clearly municipal stories). Uses
    # single-word tokens since cluster["tokens"] only ever contains
    # single words from tokenize() — multi-word strings here would
    # never match (a pre-existing pattern issue elsewhere in this file
    # too, e.g. "zona leste" below; left as-is to avoid scope creep).
    ({"prefeitura","subprefeitura","municipal","vereador","zeladoria","nunes"},
     "→ Verificar DOC-SP: ato publicado no Diário Oficial da Cidade de SP?"),
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



# ── CIVIC IMPACT (Kovach Ch.8 — why this matters to citizens) ────
_CIVIC_IMPACT = {
    "investigativo": "🔎 Possível desvio de dinheiro público",
    "privatizacao":  "🏭 Mudança em serviço público — pode afetar tarifas e acesso",
    "fiscal":        "💸 Impacto no erário — afeta investimentos em saúde e educação",
    "saude":         "🏥 Impacta acesso a serviços de saúde da população",
    "educacao":      "🎓 Afeta qualidade e acesso à educação pública",
    "seguranca":     "🚔 Envolve riscos à segurança da população",
    "obras":         "🏗️ Obra pública com recursos de impostos",
    "meio_ambiente": "🌿 Consequências para o meio ambiente e qualidade de vida",
    "disciplinar":   "⚖️ Responsabilização de servidor público",
    "legal":         "🏛️ Decisão com impacto nos direitos dos cidadãos",
    "licitacao":     "🛒 Processo que define quem presta serviços ao governo",
    "contrato":      "📝 Compromisso financeiro do Estado com empresa privada",
    "urgencia":      "🚨 Contratação sem licitação — exige justificativa pública",
    "politica":      "🏛️ Decisão que afeta as estruturas de representação política",
    "economia":      "💰 Impacto econômico para empresas e trabalhadores",
    # v1.1: added to cover categories the broken mapping never reached
    "sp_local":      "🏙️ Decisão da gestão municipal de São Paulo — afeta o dia a dia da cidade",
    "ciencia":       "🔬 Conhecimento gerado com financiamento público",
}
_CIVIC_ACTIONS = {
    "investigativo": "→ Pedido LAI pode revelar documentos completos",
    "privatizacao":  "→ Contrato completo: DOESP ou portal de transparência",
    "saude":         "→ Contratos de OS de saúde: verificar DOESP caderno Gestão",
    "obras":         "→ Licitação e contrato: DOESP ou e-negócios",
    "contrato":      "→ Extrato + CNPJ: DOESP ou portal de contratações",
    "licitacao":     "→ Edital completo: DOESP ou comprasnet",
    "disciplinar":   "→ Processo: verificar DOESP caderno Pessoal",
    "legal":         "→ Inteiro teor: portal do tribunal respectivo",
    "fiscal":        "→ Portal da Transparência federal/estadual",
    # v1.1
    "sp_local":      "→ Diário Oficial da Cidade de SP (DOC-SP) tem o ato completo",
}

# v1.1: explicit STORY_CATEGORIES label -> _CIVIC_IMPACT key mapping.
# BUG FIX: the previous code used cat_label.split()[-1].lower(), which
# broke for 11 of 13 categories due to accent mismatches ("política" vs
# "politica") and label mismatches ("São Paulo" -> "paulo", not a real
# key). That meant civic_impact()/civic_action() silently returned ""
# for almost every story except "Investigativo" and "Economia".
_CATEGORY_TO_CIVIC_KEY = {
    "Investigativo":  "investigativo",
    "Privatização":   "privatizacao",
    "Justiça/Crime":  "seguranca",
    "Judiciário":     "legal",
    "Política":       "politica",
    "Economia":       "economia",
    "São Paulo":      "sp_local",
    "Saúde":          "saude",
    "Meio Ambiente":  "meio_ambiente",
    "Ciência":        "ciencia",
    "Violência":      "seguranca",
    "Legislativo":    "politica",
    "Geral":          "",
}

def civic_impact(category):
    """Kovach Ch.8: why this story matters to the citizen."""
    return _CIVIC_IMPACT.get(category, "")

def civic_action(category):
    """Kovach Ch.10: what the citizen can do — specific tool."""
    return _CIVIC_ACTIONS.get(category, "")

def civic_key_for_label(cat_label):
    """v1.1: correct label->key lookup, replacing the broken split()[-1] hack."""
    return _CATEGORY_TO_CIVIC_KEY.get(cat_label, "")


def source_independence(cluster):
    """
    Kovach Ch.5: flag when all sources are from the same faction.
    Verification requires multiple independent witnesses.
    """
    sources = cluster["sources"]
    inv     = sources & INVESTIGATIVE_PLUS
    grande  = sources & MAINSTREAM
    n       = len(sources)
    if n >= 2:
        if not inv:
            return "_📰 Apenas grande imprensa — ângulo investigativo em aberto_"
        if not grande:
            return "_💡 Apenas fontes investigativas — grande imprensa não confirmou_"
    return None

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
    Kovach Ch.8: "Journalism is storytelling with a purpose. Distillation."
    Each card = WHO verified it + WHAT happened + WHY it matters + WHAT TO DO.
    """
    label, emoji_cov, n_sources, has_inv, has_grande = score_story(cluster)
    cat_emoji, cat_label = story_category(cluster)
    w         = extract_5w(cluster)
    rep       = pick_representative(cluster)
    impact    = civic_impact(civic_key_for_label(cat_label))
    action    = civic_action(civic_key_for_label(cat_label))
    ind_note  = source_independence(cluster)
    followups = suggest_followups(cluster, cluster["sources"])

    # v1.1: news-value axes + VIP watchlist check
    axes  = compute_news_axes(cluster)
    badge = axis_badge(axes)
    vip_term = None
    for a in cluster["articles"]:
        vip_term = check_vip_watchlist(a.title + " " + a.description)
        if vip_term: break

    # ── Header: verification tier + category (Kovach Ch.2+4) ──
    header_line = f"📰 *{date_str}* | {emoji_cov} {label} — {n_sources} fontes"
    if badge: header_line += f" | {badge}"
    lines = [header_line]
    if vip_term:
        lines.append(f"🚨 *ALERTA VIP WATCHLIST: {vip_term}*")
    lines += [
        f"{cat_emoji} *{cat_label}*",
        "━━━",
    ]

    # ── WHAT: clean distilled headline (Ch.8 — distillation) ──
    title = re.sub(r"^(EXCLUSIVO[:\s]+|EXCLUSIVA[:\s]+)", "", w["what"], flags=re.I).strip()
    title = re.sub(r"^[A-Z][A-Z]{3,}\s*[-\u2013]\s*", "", title).strip()
    if len(title) > 110: title = title[:107] + "…"
    lines.append(f"📌 *{title}*")

    # ── WHO, WHERE, WHEN (structured 5W) ──
    if w["who"]:    lines.append(f"👤 {w['who'][:80]}")
    if w["where"]:  lines.append(f"📍 {w['where']}")
    lines.append(   f"📅 {w['when']}")
    if w["value"]:  lines.append(f"💰 {w['value']}")

    # ── CONTEXT: first sentence only (distillation, not dump) ──
    if w.get("context") or w.get("why"):
        ctx_text = w.get("context") or w.get("why","")
        if len(ctx_text) > 25:
            ctx = ctx_text[:160] + ("…" if len(ctx_text)>160 else "")
            lines.append(f"💬 _{ctx}_")

    # ── WHY IT MATTERS: civic impact (Kovach Ch.8 — relevant to citizens) ──
    if impact:
        lines.append(f"\n{impact}")

    # ── SOURCES with verification note (Kovach Ch.3+4 — who verified) ──
    lines.append("━━━")
    src_parts = []
    seen_src  = set()
    for art in cluster["articles"]:
        if art.source not in seen_src:
            seen_src.add(art.source)
            lnk = f"[{art.emoji} {art.source}]({art.link})" if art.link else f"{art.emoji} {art.source}"
            src_parts.append(lnk)
    lines.append("📱 " + " · ".join(src_parts))

    # ── INDEPENDENCE NOTE (Kovach Ch.5 — warn about faction homogeneity) ──
    if ind_note:
        lines.append(ind_note)

    # ── CIVIC ACTION (Kovach Ch.10 — conscience; enable citizen watchdogism) ──
    if action or followups:
        lines.append("━━━")
        if action: lines.append(action)
        for s in followups[:1]: lines.append(s)

    return "\n".join(lines)


def build_summary(clusters, all_articles, date_str, failed_sources, geo_buckets=None):
    """
    Kovach Ch.9 (Proportion): "Journalism creates a map for citizens to navigate society.
    Its value depends on completeness and proportionality."
    Max 6 stories, max 2 per category, coverage gaps shown.
    v1.3: also shows a per-geographic-scope breakdown when geo_buckets
    is provided, since the fichas that follow are now organized that
    way (closest to farthest) rather than by verification tier alone.
    """
    n_ok  = len(SOURCES) - len(failed_sources)
    n_arts = len(all_articles)

    # Proportionality guard (Kovach Ch.9) — max 2 per category
    cat_count = {}; top = []
    for c in clusters:
        _cat_em, _cat_lb = story_category(c); cat_key = _cat_lb
        if cat_count.get(cat_key, 0) < 2:
            top.append(c); cat_count[cat_key] = cat_count.get(cat_key, 0) + 1
        if len(top) >= 6: break

    # Verification tier counts (Kovach Ch.2 — truth levels)
    verificado = sum(1 for c in clusters if score_story(c)[0].startswith("🔒"))
    relatado   = sum(1 for c in clusters if score_story(c)[0] in ("📋 MÚLTIPLAS","📋 CRUZADO","📰 RELATADO"))
    apurado    = sum(1 for c in clusters if score_story(c)[0].startswith("💡"))
    aviso      = sum(1 for c in clusters if score_story(c)[0].startswith("📡"))

    lines = [
        f"📰 *MONITOR DE NOTÍCIAS — {date_str}*",
        f"🗞️ {n_ok}/{len(SOURCES)} fontes · {n_arts} pautas relevantes",
        f"🔒 {verificado} verificadas  📋 {relatado} relatadas  💡 {apurado} apuradas  📡 {aviso} avisos",
    ]
    if geo_buckets:
        counts = " ".join(
            f"{_GEO_LABELS[k][0]} {len(geo_buckets[k])}"
            for k in ["local", "estadual", "nacional", "internacional"]
        )
        lines.append(f"_Por escopo (perto → longe): {counts}_")
    lines.append("━━━")

    for i, c in enumerate(top, 1):
        label, em, n, has_inv, has_grande = score_story(c)
        cat_em, _ = story_category(c)
        rep      = pick_representative(c)
        headline = short_headline(rep.title, rep.description)
        inv_flag = " 💡" if has_inv and not has_grande else ""

        src_links = []
        seen = set()
        for art in c["articles"]:
            if art.source not in seen:
                seen.add(art.source)
                src_links.append(f"[{art.emoji}]({art.link})" if art.link else art.emoji)
            if len(src_links) >= 4: break

        lines.append(f"{em} {cat_em} *{i}.* {headline}{inv_flag} · {n}f  " + " ".join(src_links))

    # Coverage gap report (Kovach Ch.9 — comprehensiveness)
    shown_cats = {story_category(c)[1] for c in top}  # label part like "Política"
    all_watched = {"Política","Economia","São Paulo","Saúde",
                   "Meio Ambiente","Investigativo","Judiciário","Segurança"}
    gaps_labels = all_watched - shown_cats
    gaps = gaps_labels
    if gaps:
        lines.append(f"_Sem cobertura hoje: {' · '.join(sorted(gaps))}_")

    if len(clusters) > 6:
        lines.append(f"_↓ mais {len(clusters)-6} pautas nos cards abaixo_")
    if failed_sources:
        lines.append("━━━\n⚠️ Sem resposta: " + ", ".join(failed_sources))
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
            cat_em = story_category(c)[0]
            t = clean_headline(rep.title)[:70] + ("…" if len(rep.title) > 70 else "")
            lnk = f"[{emo} {src}]({rep.link})" if rep.link else f"{emo} {src}"
            lines.append(f"  {cat_em} {lnk}: _{t}_")

    if big_solos:
        lines.append("\n*Grande imprensa (exclusivos):*")
        for c in big_solos[:5]:
            rep = pick_representative(c)
            src = list(c["sources"])[0]
            emo = next((s["emoji"] for s in SOURCES if s["name"] == src), "")
            cat_em = story_category(c)[0]
            t = clean_headline(rep.title)[:70] + ("…" if len(rep.title) > 70 else "")
            lnk = f"[{emo} {src}]({rep.link})" if rep.link else f"{emo} {src}"
            lines.append(f"  {cat_em} {lnk}: _{t}_")

    return "\n".join(lines)


# ── NEGATIVE CASE ANALYSIS (v1.1) ───────────────────────────────────
# Nowell et al. (2017): credibility requires systematically reviewing
# cases that diverge from what the pipeline surfaces, not just auditing
# what got through. is_relevant() silently drops articles every run;
# nothing samples what was dropped for a human to sanity-check the
# filter's calibration.
def sample_negative_cases(all_recent_articles, n=5):
    """
    Sample articles that were recent enough to consider but were
    filtered out by is_relevant(). Not a random sample — biased toward
    articles from investigative/specialized sources, since a false
    negative there is more costly (those outlets break stories the
    mainstream press hasn't covered yet, so losing one to an over-eager
    filter is a bigger loss than losing a mainstream wire item).
    """
    rejected = [a for a in all_recent_articles if not is_relevant(a)]
    rejected.sort(key=lambda a: 0 if a.source in INVESTIGATIVE else 1)
    return rejected[:n]


def build_negative_case_digest(rejected_sample, date_str):
    """v1.1: format the negative-case sample as a Telegram message."""
    if not rejected_sample:
        return None
    lines = [
        f"🔍 *REVISÃO DE CASOS NEGATIVOS — {date_str}*",
        "_Artigos filtrados pelo is_relevant() — vale checar se não são falsos negativos_",
        "━━━",
    ]
    for a in rejected_sample:
        t = clean_headline(a.title)[:80]
        lines.append(f"{a.emoji} {a.source}: _{t}_")
    return "\n".join(lines)


def log_selection_rate(total_recent, total_relevant):
    """
    v1.1 — Spangher et al. (2024) found 2-6% of source documents become
    "news" in their SF government-document dataset. News Monitor's
    task is different (curating across already-published news, not
    predicting coverage of raw government documents), so this isn't a
    directly comparable benchmark — logged here as a calibration
    signal for the is_relevant() filter's strictness over time, not as
    a claim that the same 2-6% target applies.
    """
    rate = (total_relevant / total_recent) if total_recent else 0
    print(f"  📊 Taxa de relevância: {rate:.1%} ({total_relevant}/{total_recent} artigos recentes)")
    return rate


def evaluate_ambiguous_story_via_llm(cluster):
    """
    v1.1 stub — LLM-assisted curation placeholder, mirroring the DOC-SP
    monitor's evaluate_ambiguous_act_via_llm(). In a future version,
    this would call the Anthropic API to judge single-source stories
    that is_relevant() let through but that don't clearly fit any
    STORY_CATEGORIES bucket, or borderline-rejected items from
    sample_negative_cases(). Returns None; not yet wired up. Requires
    ANTHROPIC_API_KEY to be set when implemented.
    """
    return None


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
    # ═══ BCB — Angular SPA, multiple sections, ONE shared Playwright session ════
    # All BCB pages follow pattern: page /foo/bar → API /api/servico/sitebcb/bar
    # They are scraped together in _fetch_bcb_all_sections()
    {
        "name": "BCB", "full": "Banco Central do Brasil",
        "emoji": "🏦", "tier": "federal",
        "fmt": "bcb_playwright",
        "home": "https://www.bcb.gov.br/noticias",
        "sections": [
            # (page_url, section_label, api_slug)
            ("https://www.bcb.gov.br/noticias",
             "Notícias", "noticias"),
            ("https://www.bcb.gov.br/estatisticas/notaseconomicofinanceiras",
             "Notas Econômico-Financeiras", "notaseconomicofinanceiras"),
            ("https://www.bcb.gov.br/estatisticas/estatisticasmonetariascredito",
             "Nota de Crédito", "estatisticasmonetariascredito"),
            ("https://www.bcb.gov.br/politicamonetaria/copom-notas",
             "COPOM — Notas", "copom-notas"),
            ("https://www.bcb.gov.br/politicamonetaria/copom-atas",
             "COPOM — Atas", "copom-atas"),
            ("https://www.bcb.gov.br/publicacoes/focus",
             "Focus — Expectativas", "focus"),
            ("https://www.bcb.gov.br/regulacao/normativos",
             "Normativos", "normativos"),
        ],
    },
    # ═══ IBGE — two complementary sources ═══════════════════════════════════════
    {
        "name": "IBGE-Releases",
        "full": "IBGE — Releases e Notas Técnicas",
        "emoji": "📊", "tier": "federal",
        "url":  "https://servicodados.ibge.gov.br/api/v3/noticias?tipo=release&qtd=30",
        "rss2": "https://servicodados.ibge.gov.br/api/v3/noticias?qtd=30",
        "fmt":  "ibge",
        "home": "https://agenciadenoticias.ibge.gov.br/",
    },
    {
        "name": "IBGE-Notícias",
        "full": "Agência de Notícias IBGE",
        "emoji": "📊", "tier": "federal",
        "url":  "https://agenciadenoticias.ibge.gov.br/feed",
        "rss2": "https://agenciadenoticias.ibge.gov.br/?feed=rss2",
        "fmt":  "rss",
        "home": "https://agenciadenoticias.ibge.gov.br/",
    },
    # ═══ CVM — news + regulatory decisions ══════════════════════════════════════
    {
        "name": "CVM-Notícias",
        "full": "Comissão de Valores Mobiliários — Notícias",
        "emoji": "📈", "tier": "federal",
        "url":  "https://www.gov.br/cvm/pt-br/assuntos/noticias/RSS",
        "rss2": "https://www.gov.br/cvm/pt-br/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/cvm/pt-br/assuntos/noticias",
    },
    {
        "name": "CVM-Decisões",
        "full": "CVM — Deliberações via @@rss.xml",
        "emoji": "📈", "tier": "federal",
        "url":  "https://www.gov.br/cvm/pt-br/assuntos/noticias/@@rss.xml",
        "rss2": "https://www.gov.br/cvm/pt-br/assuntos/noticias/RSS",
        "fmt":  "rss",
        "home": "https://www.gov.br/cvm/pt-br/assuntos/noticias",
    },
    # ═══ CADE — news + merger decisions ══════════════════════════════════════════
    {
        "name": "CADE-Notícias",
        "full": "CADE — Notícias",
        "emoji": "⚖️", "tier": "federal",
        "url":  "https://www.gov.br/cade/pt-br/assuntos/noticias/@@rss.xml",
        "rss2": "https://www.gov.br/cade/pt-br/assuntos/noticias-do-cade/@@rss.xml",
        "rss3": "https://www.gov.br/cade/pt-br/assuntos/noticias/RSS",
        "fmt":  "rss",
        "home": "https://www.gov.br/cade/pt-br/assuntos/noticias",
    },
    {
        "name": "CADE-Julgamentos",
        "full": "CADE — Julgamentos e Decisões",
        "emoji": "⚖️", "tier": "federal",
        "url":  "https://www.gov.br/cade/pt-br/assuntos/noticias/@@rss.xml",
        "rss2": "https://www.gov.br/cade/pt-br/assuntos/noticias-do-cade/@@rss.xml",
        "rss3": "https://www.gov.br/cade/pt-br/assuntos/noticias/RSS",
        "fmt":  "rss",
        "home": "https://www.gov.br/cade/pt-br/assuntos/julgamentos",
    },
    # ═══ TCU — news + acórdãos ══════════════════════════════════════════════════
    {
        "name": "TCU-Notícias",
        "full": "TCU — Notícias e Sessões",
        "emoji": "🔍", "tier": "federal",
        # TCU news confirmed at portal.tcu.gov.br/imprensa/noticias
        # RSS endpoint tested from GitHub Actions
        "url":  "https://portal.tcu.gov.br/imprensa/noticias/rss",
        "rss2": "https://portal.tcu.gov.br/rss/tcu-noticias.rss",
        "fmt":  "rss",
        "home": "https://portal.tcu.gov.br/imprensa/noticias",
    },
    {
        "name": "TCU-Acórdãos",
        "full": "TCU — Acórdãos via webservice JSON",
        "emoji": "🔍", "tier": "federal",
        # TCU has a documented JSON webservice for acórdãos:
        # https://sites.tcu.gov.br/dados-abertos/webservices-tcu/
        "url":  "https://portal.tcu.gov.br/rss/tcu-acordaos-hoje.rss",
        "fmt":  "tcu_json",
        "home": "https://portal.tcu.gov.br/jurisprudencia/acordaos/",
    },
    # ═══ STF + STJ ════════════════════════════════════════════════════════════════
    {
        "name": "STF",
        "full": "Supremo Tribunal Federal",
        "emoji": "⚖️", "tier": "federal",
        "url":  "https://noticias.stf.jus.br/feed/",
        "rss2": "https://portal.stf.jus.br/noticias/rss.asp",
        "fmt":  "rss",
        "home": "https://portal.stf.jus.br/noticias/",
    },
    {
        "name": "STJ",
        "full": "Superior Tribunal de Justiça",
        "emoji": "⚖️", "tier": "federal",
        "url":  "https://www.stj.jus.br/sites/portalp/Comunicacao/Noticias/RSS",
        "rss2": "https://www.stj.jus.br/portaldestaque/rssnoticias.asp",
        "home": "https://www.stj.jus.br/sites/portalp/Comunicacao/Noticias",
        "fmt":  "rss",
        "home": "https://www.stj.jus.br/sites/portalp/Comunicacao/Noticias",
    },
    # ═══ ANVISA — news + recalls ══════════════════════════════════════════════════
    {
        "name": "ANVISA-Notícias",
        "full": "ANVISA — Notícias",
        "emoji": "🏥", "tier": "federal",
        "url":  "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/@@rss.xml",
        "rss2": "https://www.gov.br/anvisa/pt-br/assuntos/alertas/@@rss.xml",
        "rss3": "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/RSS",
        "fmt":  "rss",
        "home": "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa",
    },
    {
        "name": "ANVISA-Recalls",
        "full": "ANVISA — Recalls e Alertas",
        "emoji": "⚠️", "tier": "federal",
        "url":  "https://www.gov.br/anvisa/pt-br/assuntos/recall/@@rss.xml",
        "rss2": "https://www.gov.br/anvisa/pt-br/assuntos/alertas-e-noticias/@@rss.xml",
        "fmt":  "rss",
        "home": "https://www.gov.br/anvisa/pt-br/assuntos/recall",
    },
    # ═══ ANS / Receita / ANEEL / ANTT / AGU / INPE / IBAMA ══════════════════════
    {
        "name": "ANS",
        "full": "ANS — Saúde Suplementar",
        "emoji": "🏥", "tier": "federal",
        "url":  "https://www.gov.br/ans/pt-br/assuntos/noticias/RSS",
        "fmt":  "rss", "home": "https://www.gov.br/ans/pt-br/assuntos/noticias",
    },
    {
        "name": "Receita Federal",
        "full": "Receita Federal",
        "emoji": "💼", "tier": "federal",
        "url":  "https://www.gov.br/receitafederal/pt-br/noticias/RSS",
        "rss2": "https://www.gov.br/receitafederal/pt-br/@@rss.xml",
        "fmt":  "rss", "home": "https://www.gov.br/receitafederal/pt-br/noticias",
    },
    {
        "name": "ANEEL",
        "full": "ANEEL — Energia Elétrica",
        "emoji": "⚡", "tier": "federal",
        "url":  "https://www.gov.br/aneel/pt-br/assuntos/noticias/@@rss.xml",
        "rss2": "https://www.gov.br/aneel/pt-br/assuntos/noticias/RSS",
        "fmt":  "rss", "home": "https://www.gov.br/aneel/pt-br/assuntos/noticias",
    },
    {
        "name": "ANTT",
        "full": "ANTT — Transportes Terrestres",
        "emoji": "🚛", "tier": "federal",
        "url":  "https://www.gov.br/antt/pt-br/assuntos/noticias/@@rss.xml",
        "rss2": "https://www.gov.br/antt/pt-br/assuntos/noticias/RSS",
        "fmt":  "rss", "home": "https://www.gov.br/antt/pt-br/assuntos/noticias",
    },
    {
        "name": "AGU",
        "full": "Advocacia-Geral da União",
        "emoji": "🏛️", "tier": "federal",
        "url":  "https://www.gov.br/agu/pt-br/comunicacao/noticias/@@rss.xml",
        "rss2": "https://www.gov.br/agu/pt-br/comunicacao/noticias/RSS",
        "fmt":  "rss", "home": "https://www.gov.br/agu/pt-br/comunicacao/noticias",
    },
    {
        "name": "INPE",
        "full": "INPE — Pesquisas Espaciais",
        "emoji": "🌿", "tier": "federal",
        "url":  "https://www.gov.br/inpe/pt-br/@@rss.xml",
        "rss2": "https://www.gov.br/inpe/pt-br/assuntos/ultimas-noticias/RSS",
        "fmt":  "rss", "home": "https://www.gov.br/inpe/pt-br/assuntos/ultimas-noticias",
    },
    {
        "name": "IBAMA",
        "full": "IBAMA — Meio Ambiente",
        "emoji": "🌿", "tier": "federal",
        "url":  "https://www.gov.br/ibama/pt-br/@@rss.xml",
        "rss2": "https://www.gov.br/ibama/pt-br/noticias/RSS",
        "fmt":  "rss", "home": "https://www.gov.br/ibama/pt-br/noticias",
    },
    # ═══ São Paulo ══════════════════════════════════════════════════════════════
    {
        "name": "TCE-SP",
        "full": "Tribunal de Contas do Estado de SP",
        "emoji": "🔍", "tier": "estadual",
        "url":  "https://www.tce.sp.gov.br/?feed=rss2",
        "rss2": "https://www.tce.sp.gov.br/feed",
        "fmt":  "rss", "home": "https://www.tce.sp.gov.br/",
    },
    {
        "name": "Seade",
        "full": "Fundação Seade — Estatísticas SP",
        "emoji": "📊", "tier": "estadual",
        "url":  "https://www.seade.gov.br/wp-json/wp/v2/posts?per_page=15&_fields=title,link,excerpt,date",
        "rss2": "https://www.seade.gov.br/feed/",
        "fmt":  "wp_api", "home": "https://www.seade.gov.br/noticias/",
    },
    {
        "name": "Agência SP",
        "full": "Agência de Notícias do Governo de SP",
        "emoji": "🏛️", "tier": "estadual",
        "url":  "https://www.agenciasp.sp.gov.br/wp-json/wp/v2/posts?per_page=15&orderby=date&order=desc&_fields=title,link,excerpt,date",
        "rss2": "https://www.agenciasp.sp.gov.br/feed/",
        "fmt":  "wp_api", "home": "https://www.agenciasp.sp.gov.br/",
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
        return _fetch_bcb_playwright(source, hoje)  # handles all BCB sections internally

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

    # TCU acórdãos webservice
    if fmt == "tcu_json" or (source.get("name","").startswith("TCU") and text.lstrip().startswith("{")):
        raw_arts = _parse_tcu_json(text, source["name"], source["emoji"])
        if raw_arts:
            return [{"title":a.title,"desc":a.description,"date":a.pub_date,"url":a.link,"cat":"Acórdão"} for a in raw_arts]

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
    Scrape ALL BCB sections in ONE shared Playwright session.
    Each section page triggers a call to /api/servico/sitebcb/{slug}.
    We intercept ALL these calls simultaneously.
    Returns list of items labeled by section.
    """
    sections = source.get("sections", [
        ("https://www.bcb.gov.br/noticias", "Notícias", "noticias")
    ])
    all_items = []
    api_data   = {}  # slug → list of items

    def on_response(response):
        url = response.url
        if "api/servico/sitebcb/" in url and response.status == 200:
            try:
                # Extract slug from URL: .../api/servico/sitebcb/{slug}?...
                slug = url.split("sitebcb/")[1].split("?")[0]
                raw  = _parse_bcb(response.text())
                if raw and slug not in api_data:
                    api_data[slug] = raw
            except Exception: pass

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"])
            ctx  = browser.new_context(user_agent=UA, locale="pt-BR")
            page = ctx.new_page()
            page.on("response", on_response)

            for page_url, label, api_slug in sections:
                try:
                    page.goto(page_url, wait_until="networkidle", timeout=20000)
                    page.wait_for_timeout(2500)
                    # If API intercepted, we have data; otherwise DOM fallback below
                except Exception as e:
                    print(f"  BCB [{label}]: {e.__class__.__name__}")

            # DOM fallback for sections where API intercept missed
            for page_url, label, api_slug in sections:
                if api_slug in api_data:
                    continue  # already got it via API
                try:
                    page.goto(page_url, wait_until="domcontentloaded", timeout=15000)
                    page.wait_for_timeout(2000)
                    dom_items = []
                    for sel in ["h3.noticias-titulo a","div.card a[href*='/detalhenoticia']",
                                ".card-noticia a",".titulo-card a","article h2 a"]:
                        els = page.query_selector_all(sel)
                        if els:
                            for el in els[:10]:
                                try:
                                    t = (el.inner_text() or "").strip()
                                    h = el.get_attribute("href") or ""
                                    if not h.startswith("http"): h = "https://www.bcb.gov.br" + h
                                    if t and len(t) > 10:
                                        dom_items.append({"title":t,"desc":"","date":"","url":h,"cat":label})
                                except: pass
                            if dom_items:
                                api_data[api_slug] = dom_items
                                break
                except Exception: pass

            ctx.close(); browser.close()

        # Aggregate, filter to today, label by section
        n_total = 0
        for page_url, label, api_slug in sections:
            raw = api_data.get(api_slug, [])
            if not raw: continue
            today = [dict(i, cat=label) for i in raw if _item_is_today(i.get("date",""), hoje)]
            all_items.extend(today)
            n_total += len(raw)

        print(f"  BCB: {n_total} total → {len(all_items)} hoje ({len(api_data)} seções) ✅")
        return all_items

    except ImportError:
        print("  BCB: Playwright não disponível")
        return []
    except Exception as e:
        print(f"  BCB err: {e}")
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

            # Group items by section (cat field) within same institution
            sections_seen = {}
            for item in items:
                cat = item.get("cat","") or ""
                sections_seen.setdefault(cat, []).append(item)

            for cat, cat_items in list(sections_seen.items())[:4]:  # max 4 sections
              for item in cat_items[:2]:  # max 2 per section
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
    print(f"=== Monitor de Notícias v1.4 - {run_str} BRT ===\n")

    ledger = NewsLedger("news_ledger.csv")
    graph  = EntityGraph("news_graph")

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
    with ThreadPoolExecutor(max_workers=10) as pool:
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

    # v1.3: selection-rate calibration signal. `recent` (from
    # cluster_articles) already excludes is_relevant()-rejected
    # articles, so we recompute the time-window-only set to get a real
    # before/after comparison, not a tautological one.
    time_filtered = [a for a in all_articles if a.is_recent(window_h)]
    log_selection_rate(len(time_filtered), len(recent))

    # v1.3: bucket multi-source clusters by geographic scope, closest to
    # farthest — purely locational grouping, independent of editorial
    # category. Order within each bucket is preserved from clusters'
    # existing sort (source-count / verification-tier descending).
    geo_buckets = {"local": [], "estadual": [], "nacional": [], "internacional": []}
    for c in multi_source:
        geo_buckets[geo_scope(c)].append(c)

    # 3. Send summary (now includes a per-scope breakdown)
    summary = build_summary(multi_source + single_source[:5], recent, run_str, failed, geo_buckets)
    send_telegram(summary)
    time.sleep(1)

    # 4. Send story cards grouped by geographic scope, closest to
    # farthest: local -> estadual -> nacional -> internacional. Each
    # bucket gets its own header message and its own cap, so no single
    # region crowds out the others — this also mechanically increases
    # total volume (up to ~27 cards vs. the previous flat cap of 12),
    # directly in response to wanting more news surfaced per run.
    print("\n  Enviando fichas por escopo geográfico...")
    sent = 0
    BUCKET_CAPS = {"local": 8, "estadual": 5, "nacional": 8, "internacional": 6}
    for scope_key in ["local", "estadual", "nacional", "internacional"]:
        bucket = geo_buckets[scope_key]
        if not bucket:
            continue
        emoji, label = _GEO_LABELS[scope_key]
        send_telegram(f"{emoji} *{label}*", silent=True)
        time.sleep(0.3)
        for rank, cluster in enumerate(bucket[:BUCKET_CAPS[scope_key]], 1):
            cat_emoji, cat_label = story_category(cluster)
            vip_term = None
            for a in cluster["articles"]:
                vip_term = check_vip_watchlist(a.title + " " + a.description)
                if vip_term: break

            card = build_story_card(cluster, rank, date_str)

            # v1.1: cross-day recurrence check (Spangher et al. 2024 —
            # recurrence is itself a newsworthiness signal)
            recur = ledger.check_recurrence(cluster, date_str)
            if recur:
                nd, dates = recur
                card += f"\n\n🔁 _Pauta em desenvolvimento — indícios de cobertura similar em {nd} dia(s) anteriores_"

            for part in split_long(card):
                send_telegram(part)
            time.sleep(0.5)
            sent += 1

            # v1.1: log to ledger for future recurrence detection
            ledger.log_hit(date_str, cluster, cat_label, vip_term=vip_term)
            # v1.2: log to entity graph (person/org/VIP relationships + timeline)
            _record_cluster_to_graph(graph, date_str, cluster)

    # VIP watchlist hits among single-source clusters also get logged
    # and surfaced even though they won't appear in the geo buckets above
    vip_single_hits = []
    for cluster in single_source:
        vip_term = None
        for a in cluster["articles"]:
            vip_term = check_vip_watchlist(a.title + " " + a.description)
            if vip_term: break
        if vip_term:
            cat_emoji, cat_label = story_category(cluster)
            card = build_story_card(cluster, 0, date_str)
            for part in split_long(card):
                send_telegram(part)
            ledger.log_hit(date_str, cluster, cat_label, vip_term=vip_term)
            _record_cluster_to_graph(graph, date_str, cluster)
            vip_single_hits.append(vip_term)

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

    # v1.1: weekly ledger digest + negative case digest (Fridays, silent —
    # periodic audit signals, not part of the daily leads routine)
    if now.weekday() == 4 and now.hour < 12:  # Friday morning run only
        weekly = ledger.weekly_summary()
        if weekly:
            send_telegram(weekly, silent=True)
        # v1.1 fix: `recent` (from cluster_articles) already excludes
        # is_relevant()-rejected articles, so sampling from it would
        # always return empty. Reuse the time-window-only set computed
        # earlier in this function.
        neg_sample = sample_negative_cases(time_filtered, n=5)
        neg_digest = build_negative_case_digest(neg_sample, date_str)
        if neg_digest:
            send_telegram(neg_digest, silent=True)

    # v1.2: VIP network digest — if any VIP watchlist entity was hit
    # today (in either the top cards or single-source exclusives), send
    # its accumulated network so far.
    for vip_term in set(vip_single_hits):
        net_msg = graph.build_network_message(vip_term)
        if net_msg:
            send_telegram(net_msg)

    # v1.2: persist graph (events already appended incrementally)
    graph.flush()

if __name__ == "__main__":
    main()
