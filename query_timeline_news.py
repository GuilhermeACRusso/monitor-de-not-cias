"""
query_timeline_news.py — Standalone entity graph query tool for News Monitor
===========================================================================
Look up any entity's full history and relationship network without
waiting for a scheduled Action run. Reads the same three CSV files the
bot maintains (graph_entities.csv, graph_relationships.csv,
graph_events.csv), so it works on your local machine as long as you
have those files — e.g. after cloning the repo or downloading them
from GitHub.

USAGE
-----
  python query_timeline.py timeline "Transwolff"
  python query_timeline.py timeline "12.345.678/0001-90"
  python query_timeline.py network "Ricardo Nunes"
  python query_timeline.py search "engenharia"
  python query_timeline.py stats

No dependencies beyond the Python standard library.
"""

import csv
import os
import re
import sys
import unicodedata
from collections import defaultdict


def normalize(t):
    return "".join(c for c in unicodedata.normalize("NFKD", t.lower())
                   if not unicodedata.combining(c))


class GraphReader:
    """Read-only view of the three graph CSVs, for local querying."""

    def __init__(self, base="news_graph"):
        self.entities_path = f"{base}_entities.csv"
        self.relationships_path = f"{base}_relationships.csv"
        self.events_path = f"{base}_events.csv"
        self.entities = {}
        self.relationships = []
        self.events = []
        self._load()

    def _load(self):
        for path, attr in [(self.entities_path, "entities"),
                            (self.relationships_path, "relationships"),
                            (self.events_path, "events")]:
            if not os.path.exists(path):
                print(f"⚠️  {path} não encontrado — execute a partir da pasta do repositório.")
                continue
            with open(path, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if attr == "entities":
                for row in rows:
                    self.entities[row["entity_id"]] = row
            else:
                setattr(self, attr, rows)

        if not self.entities:
            print("❌ Nenhum dado carregado. Verifique se você está na pasta correta")
            print("   (deve conter graph_entities.csv, graph_relationships.csv, graph_events.csv)")
            sys.exit(1)

    def find(self, query):
        """Find entity by exact CNPJ, exact name, or substring match.
        Returns a list of (entity_id, row) — usually one match, but
        returns multiple if the query is ambiguous (e.g. searching a
        common word) so the caller can disambiguate."""
        q_digits = re.sub(r"\D", "", query)
        q_norm = normalize(query)
        exact = []
        substr = []
        for eid, row in self.entities.items():
            if q_digits and len(q_digits) >= 11:
                if re.sub(r"\D", "", row.get("cnpj", "")) == q_digits:
                    exact.append((eid, row))
                    continue
            cname_norm = normalize(row["canonical_name"])
            if q_norm == cname_norm:
                exact.append((eid, row))
            elif q_norm in cname_norm or any(
                q_norm in normalize(a) for a in row.get("aliases", "").split("|") if a
            ):
                substr.append((eid, row))
        return exact if exact else substr

    def timeline(self, entity_id, limit=50):
        evs = [e for e in self.events if entity_id in e.get("entity_ids", "").split("|")]

        def sortkey(e):
            parts = e.get("date", "").split("/")
            if len(parts) == 3:
                try:
                    return (int(parts[2]), int(parts[1]), int(parts[0]))
                except ValueError:
                    pass
            return (0, 0, 0)

        evs.sort(key=sortkey)
        return evs[-limit:]

    def network(self, entity_id):
        edges = []
        for row in self.relationships:
            if row["from_entity_id"] == entity_id:
                other = self.entities.get(row["to_entity_id"])
                if other:
                    edges.append((row["rel_type"], other, row, "outgoing"))
            elif row["to_entity_id"] == entity_id:
                other = self.entities.get(row["from_entity_id"])
                if other:
                    edges.append((row["rel_type"], other, row, "incoming"))
        edges.sort(key=lambda x: -int(x[2].get("evidence_count", "1") or "1"))
        return edges

    def stats(self):
        by_type = defaultdict(int)
        for row in self.entities.values():
            by_type[row["entity_type"]] += 1
        return {
            "total_entities": len(self.entities),
            "by_type": dict(by_type),
            "total_relationships": len(self.relationships),
            "total_events": len(self.events),
        }


def cmd_timeline(g, query):
    matches = g.find(query)
    if not matches:
        print(f"❓ Nenhuma entidade encontrada para '{query}'.")
        return
    if len(matches) > 1:
        print(f"⚠️  {len(matches)} entidades correspondem a '{query}'. Refine sua busca:")
        for eid, row in matches[:10]:
            print(f"   {eid} — {row['canonical_name']} ({row['entity_type']})")
        return

    eid, entity = matches[0]
    evs = g.timeline(eid)
    print(f"\n{'='*70}")
    print(f"🕓 LINHA DO TEMPO — {entity['canonical_name']}")
    print(f"{'='*70}")
    print(f"Tipo: {entity['entity_type']} | CNPJ: {entity.get('cnpj') or '—'}")
    print(f"Primeiro registro: {entity['first_seen']} | Último: {entity['last_seen']}")
    print(f"Total de eventos: {entity['total_events']}")
    if entity.get("aliases"):
        print(f"Também aparece como: {entity['aliases']}")
    print(f"{'-'*70}")
    if not evs:
        print("(nenhum evento registrado)")
    for e in evs:
        val = f" — {e['value']}" if e.get("value") else ""
        print(f"\n📅 {e['date']} · {e['event_type']} · {e['category']}")
        print(f"   {e['description']}{val}")
        if e.get("processo"):
            print(f"   🔖 SEI {e['processo']}")
    print(f"\n{'='*70}\n")


def cmd_network(g, query):
    matches = g.find(query)
    if not matches:
        print(f"❓ Nenhuma entidade encontrada para '{query}'.")
        return
    if len(matches) > 1:
        print(f"⚠️  {len(matches)} entidades correspondem a '{query}'. Refine sua busca:")
        for eid, row in matches[:10]:
            print(f"   {eid} — {row['canonical_name']} ({row['entity_type']})")
        return

    eid, entity = matches[0]
    edges = g.network(eid)
    print(f"\n{'='*70}")
    print(f"🕸️  REDE DE RELACIONAMENTOS — {entity['canonical_name']}")
    print(f"{'='*70}")
    print(f"{len(edges)} conexão(ões) diretas\n")
    for rel_type, other, row, direction in edges:
        arrow = "→" if direction == "outgoing" else "←"
        ev_count = row.get("evidence_count", "1")
        print(f"  {arrow} [{rel_type}] {other['canonical_name']} "
              f"({other['entity_type']}, {ev_count}x, "
              f"{row['first_seen']}–{row['last_seen']})")
        if row.get("example_processo"):
            print(f"      exemplo: SEI {row['example_processo']}")
    print(f"\n{'='*70}\n")


def cmd_search(g, query):
    matches = g.find(query)
    if not matches:
        print(f"❓ Nenhum resultado para '{query}'.")
        return
    print(f"\n{len(matches)} resultado(s) para '{query}':\n")
    for eid, row in matches[:30]:
        print(f"  {eid} — {row['canonical_name']} ({row['entity_type']}, "
              f"{row['total_events']} eventos, {row['first_seen']}–{row['last_seen']})")
    print()


def cmd_stats(g):
    s = g.stats()
    print(f"\n{'='*70}")
    print("📊 ESTATÍSTICAS DO GRAFO")
    print(f"{'='*70}")
    print(f"Total de entidades: {s['total_entities']}")
    for t, n in sorted(s["by_type"].items(), key=lambda x: -x[1]):
        print(f"  {t}: {n}")
    print(f"Total de relações: {s['total_relationships']}")
    print(f"Total de eventos: {s['total_events']}")
    print(f"{'='*70}\n")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    command = sys.argv[1]
    g = GraphReader("news_graph")

    if command == "timeline" and len(sys.argv) >= 3:
        cmd_timeline(g, " ".join(sys.argv[2:]))
    elif command == "network" and len(sys.argv) >= 3:
        cmd_network(g, " ".join(sys.argv[2:]))
    elif command == "search" and len(sys.argv) >= 3:
        cmd_search(g, " ".join(sys.argv[2:]))
    elif command == "stats":
        cmd_stats(g)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
