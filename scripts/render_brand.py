#!/usr/bin/env python3
"""Render Kelvo's vector identity and PNG exports.

Requires fonttools, CairoSVG and Pillow. Supply OFL-licensed Instrument Serif
Regular and Inter Regular. Lettering is outlined; fonts are not bundled.
The atmospheric field is original deterministic procedural art, not photography.
See brand/README.md for font provenance and usage.
"""
import argparse
import base64
import html
import io
import math
from pathlib import Path
import random
import re
import xml.etree.ElementTree as ET

import cairosvg
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.ttLib import TTFont
from PIL import Image

INK = "#271B16"
PAPER = "#F4E9D8"
VERMILION = "#FF4B00"
SEA = "#9CAFA8"
# Two complementary curved fields, alternated inside a circle. No letterform.
LEFT_FIELD = "M0 0H178C84 46 84 88 128 128C172 168 172 210 78 256H0Z"
RIGHT_FIELD = "M178 0H256V256H78C172 210 172 168 128 128C84 88 84 46 178 0Z"

class Lettering:
    def __init__(self, path):
        self.font = TTFont(path)
        self.glyphs = self.font.getGlyphSet()
        self.cmap = self.font.getBestCmap()
        self.units = self.font["head"].unitsPerEm

    def width(self, text, size, tracking=0):
        advance = sum(self.font["hmtx"][self.cmap[ord(c)]][0] for c in text)
        return advance * size / self.units + max(0, len(text) - 1) * tracking

    def centered(self, text, center, baseline, size, color=INK, tracking=0):
        return self.draw(text, center - self.width(text, size, tracking) / 2,
                         baseline, size, color, tracking)

    def draw(self, text, x, baseline, size, color=INK, tracking=0):
        paths, cursor = [], 0
        scale = size / self.units
        for character in text:
            name = self.cmap[ord(character)]
            pen = SVGPathPen(self.glyphs)
            self.glyphs[name].draw(pen)
            commands = re.sub(
                r"-?\d+(?:\.\d+)?",
                lambda m: f"{float(m.group()):.3f}".rstrip("0").rstrip(".")
                if "." in m.group() else m.group(),
                pen.getCommands(),
            )
            if commands:
                paths.append(
                    f'<path transform="translate({cursor:.3f} 0)" d="{commands}"/>'
                )
            cursor += self.font["hmtx"][name][0] + tracking / scale
        return (
            f'<g aria-label="{html.escape(text, quote=True)}" fill="{color}" '
            f'transform="translate({x} {baseline}) scale({scale:.6f} {-scale:.6f})">'
            + "".join(paths) + "</g>"
        )

def mark(color=INK, x=0, y=0, size=256, key="mark", small=False):
    count, height, step = (4, 44, 60) if small else (6, 32, 38.4)
    def bands(parity):
        return "".join(
            f'<rect x="0" y="{16 + i * step:g}" width="256" height="{height}"/>'
            for i in range(parity, count, 2)
        )
    return (
        f'<g fill="{color}" transform="translate({x} {y}) scale({size / 256})">'
        f'<defs><clipPath id="{key}-outer"><circle cx="128" cy="128" r="112"/></clipPath>'
        f'<clipPath id="{key}-left"><path d="{LEFT_FIELD}"/></clipPath>'
        f'<clipPath id="{key}-right"><path d="{RIGHT_FIELD}"/></clipPath></defs>'
        f'<g clip-path="url(#{key}-outer)">'
        f'<g clip-path="url(#{key}-left)">{bands(0)}</g>'
        f'<g clip-path="url(#{key}-right)">{bands(1)}</g>'
        '</g></g>'
    )

def document(width, height, title, description, content):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 {width} {height}" '
        'role="img" aria-labelledby="title desc">\n'
        f'<title id="title">{html.escape(title)}</title>\n'
        f'<desc id="desc">{html.escape(description)}</desc>\n'
        f'{content}\n</svg>\n'
    )

def atmosphere(width, height):
    """A soft mineral/peach field with fixed-seed grain; no external imagery."""
    rng = random.Random(20261001)
    fields = []
    for density in [5, 13, 37]:
        tile = Image.new("L", (density, density))
        tile.putdata([rng.randrange(256) for _ in range(density * density)])
        fields.append(tile.resize((width, height), Image.Resampling.BICUBIC).tobytes())
    sky, cloud = (142, 165, 159), (246, 219, 194)
    pixels = bytearray(width * height * 3)
    for y in range(height):
        for x in range(width):
            index = y * width + x
            nx, ny = x / width, y / height
            billow = (fields[0][index] - 128) / 128 * 0.45
            billow += (fields[1][index] - 128) / 128 * 0.17
            billow += (fields[2][index] - 128) / 128 * 0.035
            envelope = ny - 0.12 - 0.23 * math.sin(nx * 5.3 + 0.9) + billow
            blend = min(1, max(0, envelope * 2.9 + 0.18))
            blend = blend * blend * (3 - 2 * blend)
            grain = rng.gauss(0, 3.1)
            for channel in range(3):
                value = sky[channel] * (1 - blend) + cloud[channel] * blend + grain
                pixels[index * 3 + channel] = max(0, min(255, round(value)))
    field = Image.frombytes("RGB", (width, height), bytes(pixels))
    buffer = io.BytesIO()
    field.save(buffer, format="PNG", optimize=True)
    return base64.b64encode(buffer.getvalue()).decode()

def banner(serif, height):
    width, divide = 1600, 920
    offset = (height - 720) / 2
    texture = atmosphere(divide, height)
    parts = [
        f'<rect width="1600" height="{height}" fill="{VERMILION}"/>',
        f'<image x="0" y="0" width="{divide}" height="{height}" '
        f'xlink:href="data:image/png;base64,{texture}"/>',
        mark(INK, 294, 318 + offset, 84, "banner-lockup"),
        serif.draw("KELVO", 389, 396 + offset, 100, tracking=0.5),
        mark(INK, 1016, 111 + offset, 492, "banner-bands"),
    ]
    return document(
        width, height, "Kelvo — open-source analytics by SYNEHQ.",
        "A small serif Kelvo lockup centered over a grained sea-glass and peach "
        "field. On vermilion, a large abstract circular mark is formed by six "
        "interleaved curved bands. The symbol is geometric, not a monogram.", "".join(parts),
    )

def readme_banner(serif, sans, marks_path):
    """A database grid framing Kelvo's small lockup and product headline."""
    width, height = 1600, 800
    cell_width, cell_height = 200, height / 6
    background, panel, line, text_color = "#FCFAF8", "#FFFEFD", "#DDD3D5", "#211C1C"
    # Inline the reviewed sprite: GitHub rendering must not depend on remote assets.
    ET.register_namespace("", "http://www.w3.org/2000/svg")
    sprite = ET.parse(marks_path).getroot()
    symbols = "".join(ET.tostring(child, encoding="unicode") for child in sprite)
    parts = [f'<defs>{symbols}</defs>',
             f'<rect width="{width}" height="{height}" fill="{background}"/>']

    # Fine subdivisions provide the quiet graph-paper texture of the reference.
    fine = []
    for x in range(0, width + 1, 100):
        fine.append(f'<path d="M{x} 0V{height}"/>')
    for row in range(13):
        y = row * height / 12
        fine.append(f'<path d="M0 {y:g}H{width}"/>')
    parts.append('<g fill="none" stroke="#EBE5E5" stroke-width="0.7">'
                 + "".join(fine) + '</g>')

    labels = {
        "postgresql": "PostgreSQL", "mysql": "MySQL", "mongodb": "MongoDB",
        "duckdb": "DuckDB", "clickhouse": "ClickHouse", "snowflake": "Snowflake",
        "databricks": "Databricks", "elasticsearch": "Elasticsearch",
    }

    def database(source, column, row, opacity=1):
        x, y = column * cell_width, row * cell_height
        label = labels[source]
        label_size = 18 if source != "elasticsearch" else 16
        icon_size, gap = 38, 12
        group_width = icon_size + gap + sans.width(label, label_size)
        start = x + (cell_width - group_width) / 2
        center_y = y + cell_height / 2
        # Clear the fine grid inside occupied cells while retaining major borders.
        return (
            f'<rect x="{x}" y="{y:g}" width="{cell_width}" height="{cell_height:g}" fill="{panel}"/>'
            f'<g opacity="{opacity}">'
            f'<use xlink:href="#{source}" x="{start:.3f}" y="{center_y - icon_size / 2:.3f}" '
            f'width="{icon_size}" height="{icon_size}"/>'
            + sans.draw(label, start + icon_size + gap, center_y + 6, label_size, "#514548")
            + '</g>'
        )

    # Only supported database products; no unrelated workplace integrations.
    placements = [
        ("mongodb", 1, 0, 1), ("duckdb", 6, 0, 1),
        ("postgresql", 0, 1, 1), ("mysql", 2, 1, 1),
        ("snowflake", 5, 1, 1), ("elasticsearch", 7, 1, 1),
        ("databricks", 0, 2, 1), ("duckdb", 1, 2, 0.22),
        ("mysql", 6, 2, 0.22), ("clickhouse", 7, 2, 1),
        ("elasticsearch", 1, 3, 0.9), ("snowflake", 6, 3, 0.22),
        ("mongodb", 7, 3, 0.22),
        ("postgresql", 1, 4, 0.28), ("databricks", 3, 4, 0.9),
        ("duckdb", 5, 4, 0.22), ("clickhouse", 7, 4, 0.65),
        ("duckdb", 0, 5, 0.3), ("mysql", 2, 5, 0.85),
        ("snowflake", 4, 5, 0.25), ("postgresql", 6, 5, 0.25),
    ]
    parts.extend(database(*placement) for placement in placements)
    major = []
    for x in range(0, width + 1, cell_width):
        major.append(f'<path d="M{x} 0V{height}"/>')
    for row in range(7):
        major.append(f'<path d="M0 {row * cell_height:g}H{width}"/>')
    parts.append(f'<g fill="none" stroke="{line}" stroke-width="1">'
                 + "".join(major) + '</g>')

    # Top-center signature stays compact and distinct from the product headline.
    parts.append(f'<rect x="600" y="0" width="400" height="{cell_height:g}" '
                 f'fill="{panel}" stroke="{line}"/>')
    word_size, symbol_size, gap = 48, 46, 10
    lockup_width = symbol_size + gap + serif.width("KELVO", word_size, 0.3)
    start = (width - lockup_width) / 2
    parts.extend([
        mark(VERMILION, start, 42, symbol_size, "readme-lockup"),
        serif.draw("KELVO", start + symbol_size + gap, 84, word_size,
                   "#171514", tracking=0.3),
        f'<rect x="400" y="{2 * cell_height:g}" width="800" height="{2 * cell_height:g}" '
        f'fill="{panel}" stroke="{line}"/>',
        sans.centered("Your databases.", 800, 370, 62, text_color, tracking=-2.3),
        sans.centered("One query gateway.", 800, 438, 62, text_color, tracking=-2.3),
        sans.centered("Native execution. DuckDB federation. Arrow results.",
                      800, 489, 20, "#857879", tracking=-0.3),
        f'<rect x="0.5" y="0.5" width="1599" height="799" fill="none" stroke="{line}"/>',
    ])
    return document(width, height, "Kelvo — Your databases. One query gateway.",
                    "A small red Kelvo symbol and black serif wordmark sit above a warm-white "
                    "database grid. PostgreSQL, MySQL, MongoDB, DuckDB, ClickHouse, Snowflake, "
                    "Databricks and Elasticsearch surround the heading. Native execution. "
                    "DuckDB federation. Arrow results.", "".join(parts))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serif", type=Path, required=True)
    parser.add_argument("--sans", type=Path, required=True)
    parser.add_argument("--database-marks", type=Path,
                        default=Path(__file__).resolve().parent.parent / "brand/database-marks.svg")
    parser.add_argument("--banner-only", action="store_true",
                        help="Update only the README banner SVG and PNG")
    parser.add_argument("--output", type=Path, default=Path("brand"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    serif = Lettering(args.serif)
    header = readme_banner(serif, Lettering(args.sans), args.database_marks)
    (args.output / "kelvo-banner.svg").write_text(header)
    cairosvg.svg2png(
        bytestring=header.encode(), write_to=str(args.output / "kelvo-banner.png"),
    )
    if args.banner_only:
        print(f"Wrote the Kelvo README banner SVG and PNG to {args.output}")
        return
    for suffix, color in [("", INK), ("-inverse", PAPER)]:
        (args.output / f"kelvo-mark{suffix}.svg").write_text(document(
            256, 256, "Kelvo mark", "Six interleaved curved bands form an abstract circular symbol.",
            mark(color),
        ))
        (args.output / f"kelvo-wordmark{suffix}.svg").write_text(document(
            760, 208, "Kelvo", "Kelvo's abstract circular symbol and editorial serif wordmark.",
            mark(color, 0, 0, 208, "lockup-bands")
            + serif.draw("KELVO", 236, 177, 218, color, tracking=0.4),
        ))
    (args.output / "kelvo-mark-small.svg").write_text(document(
        256, 256, "Kelvo small mark", "Four interleaved curved bands for sizes below 32 pixels.",
        mark(small=True),
    ))
    social = banner(serif, 840)
    (args.output / "kelvo-social.svg").write_text(social)
    cairosvg.svg2png(
        bytestring=social.encode(), write_to=str(args.output / "kelvo-social.png"),
        output_width=1200, output_height=630,
    )
    icon = document(512, 512, "Kelvo", "Kelvo's abstract circular symbol in ink on vermilion.",
                    f'<rect width="512" height="512" fill="{VERMILION}"/>'
                    + mark(INK, 32, 32, 448, "icon-bands"))
    cairosvg.svg2png(bytestring=icon.encode(), write_to=str(args.output / "kelvo-icon.png"))
    print(f"Wrote ten Kelvo brand assets to {args.output}")

if __name__ == "__main__":
    main()
