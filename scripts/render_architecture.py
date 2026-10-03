#!/usr/bin/env python3
"""Render the README architecture board using Kelvo's existing grid identity.

Run on a build host with the same fonts/dependencies as render_brand.py. All
lettering and licensed product marks are embedded vectors; no remote resources
are requested by the SVG. PNG exports use the same composition at 2x resolution.
"""
import argparse
import json
from pathlib import Path
import xml.etree.ElementTree as ET

import cairosvg
from PIL import Image
from render_brand import Lettering, document, mark

W, H = 1600, 1800
BG, WHITE, GRID, FINE = '#FCFAF8', '#FFFEFD', '#DDD3D5', '#EEE8E7'
INK, MUTED, ORANGE, SOFT = '#211C1C', '#756A69', '#FF4B00', '#FFF2E9'


class Board:
    def __init__(self, serif, sans):
        self.serif, self.sans = serif, sans
        self.parts, self.labels, self.errors = [], [], []

    def add(self, value):
        self.parts.append(value)

    def rect(self, x, y, w, h, fill=WHITE, stroke=GRID, dashed=False):
        dash = ' stroke-dasharray="7 6"' if dashed else ''
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" stroke="{stroke}" stroke-width="1.25"{dash}/>')

    def text(self, value, x, y, size=22, color=INK, width=None, center=False, tracking=0, serif=False):
        font = self.serif if serif else self.sans
        measured = font.width(value, size, tracking)
        if width is not None and measured > width:
            self.errors.append(f'Text exceeds its cell: {value!r}: {measured:.1f} > {width}')
        left = x - measured / 2 if center else x
        if not 0 <= left <= W or left + measured > W or not 0 < y < H:
            self.errors.append(f'Text exceeds artwork: {value!r}')
        self.labels.append({'text': value, 'x': round(left, 2), 'baseline': y, 'size': size, 'width': round(measured, 2)})
        self.add(font.draw(value, left, y, size, color, tracking))

    def line(self, points, color=INK, dashed=False, end=True, start=False, weight=1.8):
        path = 'M' + ' L'.join(f'{x} {y}' for x, y in points)
        attrs = (' stroke-dasharray="6 6"' if dashed else '')
        key = 'orange' if color == ORANGE else 'ink'
        if end:
            attrs += f' marker-end="url(#{key}-arrow)"'
        if start:
            attrs += f' marker-start="url(#{key}-arrow)"'
        self.add(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="{weight}" stroke-linejoin="round"{attrs}/>')

    def logo(self, name, x, y, size=42):
        self.add(f'<use xlink:href="#{name}" x="{x}" y="{y}" width="{size}" height="{size}"/>')

    def icon(self, name, x, y, size=40, color=INK):
        shapes = {
            'code': '<path d="M12 11 3 20l9 9m16-18 9 9-9 9M24 5 16 35"/>',
            'notebook': '<rect x="5" y="4" width="30" height="29"/><path d="M5 12h30M12 18h8m-8 6h16M1 37h38"/>',
            'jobs': '<rect x="2" y="14" width="10" height="12"/><rect x="28" y="2" width="10" height="12"/><rect x="28" y="26" width="10" height="12"/><path d="M12 20h8V8h8M20 20v12h8"/>',
            'chart': '<path d="M4 3v33h33"/><rect x="10" y="23" width="5" height="13"/><rect x="21" y="14" width="5" height="22"/><rect x="32" y="6" width="5" height="30"/>',
            'file': '<path d="M8 2h17l9 9v27H8ZM25 2v10h9M14 20h14M14 26h14M14 32h9"/>',
        }
        self.add(f'<g transform="translate({x} {y}) scale({size / 40})" fill="none" stroke="{color}" stroke-width="1.6" stroke-linecap="square" stroke-linejoin="miter">{shapes[name]}</g>')


def render(serif, sans, database_marks, architecture_marks):
    b = Board(serif, sans)
    symbols = []
    identifiers = set()
    for source in (database_marks, architecture_marks):
        for child in ET.parse(source).getroot():
            if child.tag.rsplit('}', 1)[-1] != 'symbol':
                continue
            identifier = child.attrib['id']
            if identifier in identifiers:
                raise ValueError('Duplicate product symbol')
            identifiers.add(identifier)
            symbols.append(ET.tostring(child, encoding='unicode'))
    arrows = ''.join(f'<marker id="{key}-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M1 1 9 5 1 9" fill="none" stroke="{color}" stroke-width="1.5"/></marker>' for key, color in [('ink', INK), ('orange', ORANGE)])
    b.add('<defs>' + ''.join(symbols) + arrows + '</defs>')
    b.rect(0, 0, W, H, BG, GRID)
    for x in range(0, W + 1, 100):
        b.line([(x, 0), (x, H)], GRID if x % 200 == 0 else FINE, end=False, weight=.8)
    for y in range(0, H + 1, 60):
        b.line([(0, y), (W, y)], GRID if y % 120 == 0 else FINE, end=False, weight=.8)

    # Match the banner's intentionally small, centered symbol and serif wordmark.
    b.rect(400, 0, 800, 228, WHITE)
    b.add(mark(ORANGE, 705, 36, 48, 'architecture-lockup'))
    b.text('KELVO', 768, 82, 54, width=150, serif=True)
    b.text('ARCHITECTURE', 40, 73, 15, MUTED, tracking=1.5)
    b.text('BY SYNEHQ', 1420, 73, 15, MUTED, width=140, tracking=1.1)
    b.rect(200, 116, 1200, 112, WHITE)
    b.text('From your data to your next decision.', 800, 168, 47, width=1120, center=True)
    b.text('Keep your databases. Connect the tools you already use.', 800, 207, 23, MUTED, width=1120, center=True)

    # Request entry. This is caller software, not a built-in SQL editor/BI app.
    for i, (icon, title, subtitle) in enumerate([
        ('code', 'Apps & APIs', 'Your product and backend'),
        ('notebook', 'Notebooks & agents', 'Your analysis code'),
        ('jobs', 'SQL & job runners', 'Your editors and workflows'),
    ]):
        x = 400 + i * 800 / 3
        b.rect(x, 260, 800 / 3, 100)
        b.icon(icon, x + 16, 277, 28)
        b.text(title, x + 57, 296, 20, width=198)
        b.text(subtitle, x + 18, 332, 17, MUTED, width=238)
    b.line([(800, 360), (800, 420)])
    b.rect(601, 376, 398, 27, BG, BG)
    b.text('HTTP API / CLI · request + source IDs', 800, 396, 18, MUTED, width=386, center=True)

    b.text('YOUR DATA', 40, 402, 16, MUTED, tracking=1.5)
    b.text('KELVO', 456, 402, 16, MUTED, tracking=1.5)
    b.text('YOUR ANALYTICS', 1240, 402, 16, MUTED, tracking=1.5)

    # Real, unmodified source product logos. The matrix in README documents scope.
    sources = [('postgresql', 'PostgreSQL'), ('mysql', 'MySQL'),
               ('mongodb', 'MongoDB'), ('clickhouse', 'ClickHouse'),
               ('snowflake', 'Snowflake'), ('databricks', 'Databricks'),
               ('elasticsearch', 'Elasticsearch'), ('googlebigquery', 'BigQuery')]
    for i, (identifier, label) in enumerate(sources):
        x, y = 40 + (i % 2) * 160, 420 + (i // 2) * 120
        b.rect(x, y, 160, 120)
        b.logo(identifier, x + 59, y + 18, 42)
        b.text(label, x + 80, y + 94, 19, width=146, center=True)
    b.rect(40, 900, 320, 44)
    b.text('More native sources + adapters', 200, 927, 17, MUTED, width=302, center=True)
    b.rect(40, 960, 320, 138)
    b.icon('file', 61, 982, 28)
    b.text('Files & attachments', 102, 1005, 22, width=240)
    b.text('CSV · Parquet · DuckDB · SQLite', 62, 1040, 18, MUTED, width=280)
    b.text('Coverage varies by path.', 62, 1073, 18, MUTED, width=281)

    # Coordinator and selected per-query process are distinct boundaries.
    b.rect(432, 420, 736, 168)
    b.text('Query gateway', 456, 463, 30, width=668)
    b.text('Go coordinator / cluster node', 456, 495, 20, MUTED, width=650)
    b.line([(800, 516), (800, 569)], GRID, end=False, weight=1)
    b.text('Catalog + selected credentials', 456, 540, 20, width=324)
    b.text('Env refs; optional private node files', 456, 567, 16, MUTED, width=324)
    b.text('Resource + source admission', 824, 540, 20, width=320)
    b.text('Query limits + optional node controls', 824, 567, 16, MUTED, width=320)
    b.line([(800, 588), (800, 634)])
    b.rect(432, 634, 736, 464, BG, '#AFA19F', dashed=True)
    b.text('Disposable query process', 456, 674, 24, width=570)
    b.text('ONE MODE PER QUERY', 962, 674, 14, MUTED, width=182, tracking=.4)
    b.rect(456, 696, 688, 114)
    b.icon('code', 480, 724, 34)
    b.text('Native execution', 538, 742, 26, width=582)
    b.text('Run the query in the source database.', 538, 780, 21, MUTED, width=582)
    b.text('OR', 800, 831, 16, MUTED, center=True, tracking=1)
    b.rect(456, 840, 688, 220)
    b.logo('duckdb', 480, 865, 42)
    b.text('Local DuckDB execution', 544, 901, 28, width=578)
    b.text('Joins, CTEs, windows and aggregates.', 480, 939, 22, MUTED, width=640)
    b.rect(480, 962, 304, 73, BG)
    b.text('Opt-in live bridge + files', 500, 991, 21, width=266)
    b.text('8 bridge adapters + file sources', 500, 1018, 17, MUTED, width=266)
    b.rect(808, 962, 312, 73, SOFT, '#F1CDBB')
    b.text('Parquet snapshots', 830, 991, 21, width=270)
    b.text('Pinned dataset generations', 830, 1018, 16, MUTED, width=270)
    b.text('Selected credentials · deadlines + output limits · private workspace', 456, 1086, 17, MUTED, width=680)

    # Source-side access, local inputs and output fan-in are kept spatially apart.
    b.line([(360, 680), (398, 680), (398, 754), (456, 754)], start=True)
    b.line([(200, 944), (200, 952), (388, 952), (388, 998), (480, 998)])
    b.line([(360, 1030), (410, 1030), (410, 998)], end=False)
    b.add(f'<circle cx="410" cy="998" r="3" fill="{INK}"/>')
    b.line([(1144, 754), (1196, 754)], ORANGE, end=False)
    b.line([(1144, 924), (1196, 924), (1196, 506), (1240, 506)], ORANGE)
    b.add(f'<circle cx="1196" cy="754" r="3.5" fill="{ORANGE}"/>')

    # Results feed user-built experiences and compatible consumers.
    b.rect(1240, 420, 320, 174)
    b.logo('apachearrow', 1379, 443, 42)
    b.text('Arrow IPC', 1400, 529, 31, width=276, center=True)
    b.text('Typed batches · optional LZ4', 1400, 567, 18, MUTED, width=286, center=True)
    b.line([(1400, 594), (1400, 607), (1580, 607), (1580, 1025)], ORANGE, end=False)
    b.rect(1249, 612, 302, 29, BG, BG)
    b.text('HTTP response or .arrow file', 1400, 634, 18, MUTED, width=293, center=True)
    consumers = [('python', 'Notebooks & agents', 'Python / PyArrow'),
                 ('chart', 'Apps & dashboards', 'Use the API in your product'),
                 ('jobs', 'Reports & workflows', 'Consume Arrow in your stack')]
    for i, (symbol, title, subtitle) in enumerate(consumers):
        y = 660 + i * 146
        b.rect(1240, y, 320, 146)
        b.line([(1580, y + 73), (1560, y + 73)], ORANGE)
        b.add(f'<circle cx="1580" cy="{y + 73}" r="3" fill="{ORANGE}"/>')
        if symbol == 'python':
            b.logo(symbol, 1380, y + 16, 40)
        else:
            b.icon(symbol, 1380, y + 16, 40)
        b.text(title, 1400, y + 94, 22, width=286, center=True)
        b.text(subtitle, 1400, y + 126, 17.5, MUTED, width=286, center=True)

    # A refresh publishes a complete generation; acquisition checks policy and age.
    b.rect(40, 1128, 1520, 176)
    b.text('OPTIONAL / DATASET ACCELERATION', 64, 1160, 16, MUTED, tracking=.8)
    stages = [
        ('Full refresh query', 'Manual, scheduled or queued', 'Reads configured source data'),
        ('Schema contract', 'Strict; optional forward evolution', 'One schema per generation'),
        ('Immutable Parquet', 'Single file or multipart', 'Local · S3 / R2 · GCS · Azure Blob'),
        ('Pinned DuckDB read', 'Freshness + policy checks', 'No automatic source fallback'),
    ]
    for i, (title, first, second) in enumerate(stages):
        x = 64 + i * 372
        b.text(title, x, 1207, 24, width=344)
        b.text(first, x, 1241, 18, MUTED, width=344)
        b.text(second, x, 1274, 18, MUTED, width=344)
        if i < len(stages) - 1:
            b.line([(x + 327, 1199), (x + 356, 1199)], ORANGE)
    b.line([(1410, 1128), (1410, 1115), (1155, 1115), (1155, 998), (1120, 998)], ORANGE)

    # Dashed request/state path; solid Arrow results bypass the broker.
    b.rect(40, 1336, 1520, 160)
    b.text('OPTIONAL / CLUSTER MODE', 64, 1368, 16, MUTED, tracking=.8)
    b.text('Gateway replicas', 84, 1415, 24, width=295)
    b.text('Tenant authentication + admission', 84, 1443, 16, MUTED, width=320)
    b.logo('nats', 600, 1388, 44)
    b.text('NATS JetStream', 666, 1415, 24, width=365)
    b.text('Tenant KV, state and durable dispatch', 600, 1443, 16, MUTED, width=414)
    b.text('Tenant-bound workers', 1110, 1415, 24, width=417)
    b.text('Each query stays on one worker', 1110, 1443, 16, MUTED, width=418)
    b.line([(402, 1406), (564, 1406)], dashed=True)
    b.text('query IDs', 483, 1393, 16, MUTED, width=152, center=True)
    b.line([(974, 1406), (1075, 1406)], dashed=True)
    b.line([(1320, 1450), (1320, 1478), (220, 1478), (220, 1450)], ORANGE)
    b.rect(540, 1462, 474, 29, WHITE, WHITE)
    b.text('Arrow over mTLS · directly to gateway', 777, 1483, 18, ORANGE, width=468, center=True)

    # Operator recovery is separate from live serving and worker diagnostics.
    b.rect(40, 1532, 744, 176)
    b.text('LOCAL / BACKUP + RECOVERY', 64, 1564, 16, MUTED, tracking=.8)
    b.text('Verified local backup', 64, 1610, 23, width=280)
    b.line([(321, 1602), (364, 1602)], ORANGE)
    b.text('Fresh recovery root', 388, 1610, 23, width=372)
    b.text('Operator command · single / multipart · Linux', 64, 1647, 19, MUTED, width=696)
    b.text('Keeps schema, policy and original refresh time.', 64, 1680, 18, MUTED, width=696)
    b.rect(816, 1532, 744, 176)
    b.text('WORKER / OPERATIONS', 840, 1564, 16, MUTED, tracking=.8)
    b.text('Metrics · readiness · phased drain', 840, 1610, 24, width=696)
    b.text('Optional lifecycle traces + bounded history', 840, 1647, 20, MUTED, width=696)
    b.text('Protected diagnostics · required-dataset readiness', 840, 1680, 18, MUTED, width=696)

    b.rect(0, 1740, W, 60, BG, GRID)
    b.text('Developer preview · DuckDB completes execution before Arrow delivery', 800, 1777, 21, MUTED, width=1520, center=True)
    description = ('Kelvo by SYNEHQ connects application, notebook and job requests to registered databases. '
        'A Go coordinator applies source configuration, query limits and optional node resource and source admission. '
        'The trusted parent resolves only selected source credential references from the environment or optional private node files. '
        'A disposable process runs one execution mode: native queries at one source, or local DuckDB SQL over supported live inputs, '
        'files and pinned Parquet snapshots. The optional live bridge has eight built-in adapters; native coverage does not imply federation. '
        'Both modes return Arrow IPC with optional LZ4 compression. Full refresh acceleration checks a schema contract and publishes '
        'a complete immutable single-file or multipart Parquet generation to local or optional cloud storage. DuckDB reads pin generations '
        'after freshness and policy checks, without automatic source fallback. Optional cluster mode uses NATS JetStream for tenant state '
        'and dispatch; Arrow results flow directly from tenant workers to gateways over mTLS. Each query executes on one worker. '
        'Operator-only Linux backup and recovery copy a verified local generation into a fresh root while preserving its original refresh time. '
        'Worker operations include protected metrics, dataset readiness and phased drain, with optional lifecycle traces and bounded history. '
        'Live federation and independent datasets have no global transaction. DuckDB execution materializes before Arrow delivery.')
    if b.errors:
        raise ValueError('\n'.join(b.errors))
    return document(W, H, 'Kelvo architecture — your data, your software, one bridge', description, ''.join(b.parts)), b.labels


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--serif', required=True, type=Path)
    parser.add_argument('--sans', required=True, type=Path)
    parser.add_argument('--database-marks', type=Path, default=Path('brand/database-marks.svg'))
    parser.add_argument('--architecture-marks', type=Path, default=Path('brand/architecture-marks.svg'))
    parser.add_argument('--output', type=Path, default=Path('brand'))
    args = parser.parse_args()
    ET.register_namespace('', 'http://www.w3.org/2000/svg')
    svg, labels = render(Lettering(args.serif), Lettering(args.sans), args.database_marks, args.architecture_marks)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'kelvo-architecture.svg').write_text(svg)
    target = args.output / 'kelvo-architecture.png'
    cairosvg.svg2png(bytestring=svg.encode(), write_to=str(target), output_width=W * 2, output_height=H * 2)
    with Image.open(target) as image:
        image.convert('RGB').save(target, optimize=True)
    (args.output / 'architecture-layout.json').write_text(json.dumps({'width': W, 'height': H, 'labels': labels}, indent=2) + '\n')
    print(json.dumps({'vector': 'kelvo-architecture.svg', 'png_dimensions': [W * 2, H * 2], 'outlined_labels': len(labels)}))


if __name__ == '__main__':
    main()
