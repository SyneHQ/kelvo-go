# Kelvo identity

![Kelvo — Your databases. One query gateway.](kelvo-banner.png)

An editorial identity for a technical product: a small serif lockup, an abstract circular symbol, and vermilion. Six bands alternate between two curved fields inside a circle. The interleaving suggests data brought together; the mark is geometric, not a letter or monogram.

The README banner places the red symbol and black wordmark at the top center of a warm-white database grid. Supported database logos frame the heading, **Your databases. One query gateway.** The social artwork retains its mineral-and-peach atmosphere.

The product name is **Kelvo**. The organization is **SYNEHQ**. Use **Kelvo by SYNEHQ** when both appear together. The uppercase wordmark is artwork; use normal title case in prose.

## Assets

| Asset | Use |
| --- | --- |
| [Primary mark](kelvo-mark.svg) · [Inverse mark](kelvo-mark-inverse.svg) | Six-band circular symbol, transparent vector, 32 px and larger |
| [Small mark](kelvo-mark-small.svg) | Four-band optical variant for 16–31 px; recolor to paper on dark surfaces |
| [Wordmark](kelvo-wordmark.svg) · [Inverse wordmark](kelvo-wordmark-inverse.svg) | Horizontal symbol and outlined serif lockup |
| [Banner SVG](kelvo-banner.svg) · [Banner PNG](kelvo-banner.png) | 1600 × 800 database-grid README artwork |
| [Database marks](database-marks.svg) · [Sources and licenses](database-marks.SOURCES.md) | Eight database symbols used in the README banner |
| [Architecture SVG](kelvo-architecture.svg) · [Architecture PNG](kelvo-architecture.png) | 1600 × 1800 editable vector board; 3200 × 3600 PNG for the README |
| [Architecture marks](architecture-marks.svg) · [Sources and licenses](architecture-marks.SOURCES.md) | BigQuery, Apache Arrow, Python and NATS symbols used with the existing database marks |
| [Social SVG](kelvo-social.svg) · [Social PNG](kelvo-social.png) | 1600 × 840 master; 1200 × 630 export |
| [Square icon](kelvo-icon.png) | 512 × 512 ink mark on vermilion |

SVG lettering and marks are paths: no installed fonts or network requests are needed to display them. The README banner is entirely vector artwork, including its embedded database symbols. The social SVG embeds its grain field as a PNG; its lettering and mark remain vectors. PNG exports provide consistent rendering in GitHub and social clients.

The architecture board extends the banner's warm-white grid into a workflow: sources on the left, the Go coordinator and two execution modes in the center, and Arrow consumers on the right. The coordinator shows selected credential resolution and resource/source admission. Vermilion arrows carry results and dataset transitions; dashed arrows mark cluster dispatch. Separate rows follow full refresh through schema contracts and immutable generations to pinned DuckDB reads, explain cluster routing, and distinguish verified local recovery from worker operations. The [architecture walkthrough](../docs/architecture.md) is the accessible text companion and documents scope, optional controls and execution boundaries.

## Color

| Color | Hex | Role |
| --- | --- | --- |
| Vermilion | `#FF4B00` | README symbol, social panel, and icon background |
| Ink | `#271B16` | Primary mark and lettering |
| Paper | `#F4E9D8` | Inverse mark and warm neutral surfaces |
| Sea glass | `#9CAFA8` | Supporting atmosphere |
| Warm white | `#FCFAF8` | README grid background |
| Rose gray | `#DDD3D5` | README grid lines |

Use ink on vermilion or paper. Use paper on ink. On the README banner, use vermilion for the symbol and near-black for the wordmark. Keep grain behind artwork, never inside the standalone mark. The social artwork's atmosphere is original procedural artwork, not a photograph or an extracted reference asset.

## Typography and spacing

**Instrument Serif Regular** supplies the wordmark. **Inter Regular** supplies the README headline, supporting line, and database labels. **Space Mono Regular** is the supporting typeface for technical content outside the logo artwork. All are SIL Open Font License 1.1 fonts. Keep the serif at its native width; do not stretch or artificially condense it.

Keep at least one band height of clear space around the visible mark. Keep the lockup at least 160 px wide in the source artwork. Below 32 px, use the four-band small mark to preserve the curved fields and negative space. Banner lockups are deliberately small and centered, with generous space. Keep the README headline and its single supporting line separate from the logo. The database grid illustrates connector coverage; it does not imply provider endorsement or universal federation support.

Preserve proportions, band count, and the supplied curves. Use the inverse assets on dark surfaces. Avoid outlines, drop shadows, extra colors, or gradients applied to the mark itself. Do not imply that a community project is an official SYNEHQ release.

## Reproduce the artwork

Run the renderer on a build host with Python 3, Pillow 11.3.0, fonttools 4.60.1, and CairoSVG 2.8.2. It uses a fixed grain seed for the social artwork and generates all ten Kelvo exports. Database symbols are read from the checked-in sprite.

```sh
python scripts/render_brand.py \
  --serif /path/to/InstrumentSerif-Regular.ttf \
  --sans /path/to/Inter.ttf \
  --output brand
```

Add `--banner-only` to regenerate only `kelvo-banner.svg` and `kelvo-banner.png`. The default database sprite is `brand/database-marks.svg`; use `--database-marks` to override it.

Generate the architecture diagram separately, using the same fonts and dependencies:

```sh
python scripts/render_architecture.py \
  --serif /path/to/InstrumentSerif-Regular.ttf \
  --sans /path/to/Inter.ttf \
  --database-marks brand/database-marks.svg \
  --architecture-marks brand/architecture-marks.svg \
  --output artifacts/architecture
```

The renderer checks label widths and emits the SVG, 2× PNG, and `architecture-layout.json` with text positions for review. Copy only the reviewed SVG and PNG into `brand/`. Edit labels and layout in `scripts/render_architecture.py`; the exported SVG is also editable in vector design tools.

Instrument Serif and Inter are supplied to the renderer; font binaries are not bundled in this repository. Inter's variable font is used at its default regular weight. Upstream typeface sources and licenses:

- [Instrument Serif Regular](https://github.com/google/fonts/blob/main/ofl/instrumentserif/InstrumentSerif-Regular.ttf) · [OFL](https://github.com/google/fonts/blob/main/ofl/instrumentserif/OFL.txt)
- [Inter Regular, pinned source](https://github.com/google/fonts/blob/0b58fb370093f9a9f4ff785d94405710b79de67c/ofl/inter/Inter%5Bopsz%2Cwght%5D.ttf) · [OFL](https://github.com/google/fonts/blob/0b58fb370093f9a9f4ff785d94405710b79de67c/ofl/inter/OFL.txt)
- [Space Mono Regular](https://github.com/google/fonts/blob/main/ofl/spacemono/SpaceMono-Regular.ttf) · [OFL](https://github.com/google/fonts/blob/main/ofl/spacemono/OFL.txt)

SHA-256 of the fonts used for these exports:

```text
InstrumentSerif-Regular.ttf
498efd461f6ddfcb7a111bf9a565709d2085d48201d501ead960d93e84ffbb88
Inter.ttf
29160a80ff49ddcab2c97711247e08b1fab27a484a329ce8b813d820dc559031
```

Kelvo artwork and the renderer are included under the repository's [Apache-2.0 license](../LICENSE). That license does not grant trademark rights. Font software remains under its upstream OFL terms. Product symbols retain their upstream licenses and trademark ownership, documented for the [database marks](database-marks.SOURCES.md) and [architecture marks](architecture-marks.SOURCES.md).
