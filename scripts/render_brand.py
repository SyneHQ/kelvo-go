#!/usr/bin/env python3
"""Render Kelvo's vector identity and PNG exports.

Requires fonttools, CairoSVG and Pillow. Supply OFL-licensed Instrument Serif
Regular and Space Mono Regular. Lettering is outlined; fonts are not bundled.
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

import cairosvg
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.ttLib import TTFont
from PIL import Image

INK = "#271B16"
PAPER = "#F4E9D8"
VERMILION = "#FF4B00"
SEA = "#9CAFA8"
# A curved, expanding K silhouette. Seven horizontal cuts retain its outline.
SILHOUETTE = (
    "M24 24H64V103C92 85 116 55 146 24H226"
    "C184 67 147 107 106 128"
    "C150 149 194 189 232 232H149"
    "C115 192 89 160 64 152V232H24Z"
)

class Lettering:
    def __init__(self, path):
        self.font = TTFont(path)
        self.glyphs = self.font.getGlyphSet()
        self.cmap = self.font.getBestCmap()
        self.units = self.font["head"].unitsPerEm

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

def mark(color=INK, x=0, y=0, size=256, key="mark"):
    return (
        f'<g fill="{color}" transform="translate({x} {y}) scale({size / 256})">'
        f'<defs><clipPath id="{key}"><path d="{SILHOUETTE}"/></clipPath></defs>'
        f'<g clip-path="url(#{key})">'
        + "".join(f'<rect x="0" y="{24 + i * 31}" width="256" height="22"/>'
                  for i in range(7))
        + '</g></g>'
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

def banner(serif, mono, height):
    width, divide = 1600, 920
    offset = (height - 720) / 2
    texture = atmosphere(divide, height)
    parts = [
        f'<rect width="1600" height="{height}" fill="{VERMILION}"/>',
        f'<image x="0" y="0" width="{divide}" height="{height}" '
        f'xlink:href="data:image/png;base64,{texture}"/>',
        mono.draw("OPEN-SOURCE ANALYTICS", 72, 65, 15, tracking=1.1),
        mono.draw("BY SYNEHQ", 1000, 65, 15, tracking=1.1),
        serif.draw("KELVO", 85, 399 + offset, 245, tracking=0.5),
        serif.draw("SQL in. Arrow out.", 94, 477 + offset, 46),
        mono.draw("GO / DUCKDB / APACHE ARROW", 76, height - 56, 14, tracking=0.35),
        mark(INK, 1016, 111 + offset, 492, "banner-bands"),
        mono.draw("BUILT FOR ANALYSIS.", 1000, height - 56, 14, tracking=0.35),
    ]
    return document(
        width, height, "Kelvo — SQL in. Arrow out.",
        "An open-source analytics gateway by SYNEHQ. A tall editorial wordmark "
        "over a grained sea-glass and peach field, beside a seven-band curved K "
        "on vermilion. Built with Go, DuckDB and Apache Arrow.", "".join(parts),
    )

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serif", type=Path, required=True)
    parser.add_argument("--mono", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("brand"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    serif, mono = Lettering(args.serif), Lettering(args.mono)
    for suffix, color in [("", INK), ("-inverse", PAPER)]:
        (args.output / f"kelvo-mark{suffix}.svg").write_text(document(
            256, 256, "Kelvo mark", "Seven horizontal bands form a curved uppercase K.",
            mark(color),
        ))
        (args.output / f"kelvo-wordmark{suffix}.svg").write_text(document(
            760, 208, "Kelvo", "Kelvo's banded K and editorial serif wordmark.",
            mark(color, 0, 0, 208, "lockup-bands")
            + serif.draw("KELVO", 236, 177, 218, color, tracking=0.4),
        ))
    (args.output / "kelvo-mark-small.svg").write_text(document(
        256, 256, "Kelvo small mark", "A solid curved K for sizes below 32 pixels.",
        f'<path fill="{INK}" d="{SILHOUETTE}"/>',
    ))
    header = banner(serif, mono, 720)
    (args.output / "kelvo-banner.svg").write_text(header)
    cairosvg.svg2png(
        bytestring=header.encode(), write_to=str(args.output / "kelvo-banner.png"),
    )
    social = banner(serif, mono, 840)
    (args.output / "kelvo-social.svg").write_text(social)
    cairosvg.svg2png(
        bytestring=social.encode(), write_to=str(args.output / "kelvo-social.png"),
        output_width=1200, output_height=630,
    )
    icon = document(512, 512, "Kelvo", "Kelvo's banded K in ink on vermilion.",
                    f'<rect width="512" height="512" fill="{VERMILION}"/>'
                    + mark(INK, 32, 32, 448, "icon-bands"))
    cairosvg.svg2png(bytestring=icon.encode(), write_to=str(args.output / "kelvo-icon.png"))
    print(f"Wrote ten Kelvo brand assets to {args.output}")

if __name__ == "__main__":
    main()
