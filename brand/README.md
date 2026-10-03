# Kelvo identity

![Kelvo — Your databases. One query gateway.](kelvo-banner.png)

Use **Kelvo** for the product, **SYNEHQ** for the organization, and **Kelvo by SYNEHQ** together. The six-band circular mark is geometric, not a letter. Keep the serif wordmark small and centered in banner layouts.

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

SVG lettering and marks are paths and need no fonts/network at display time. The social SVG embeds a grain PNG. Use PNG exports for consistent GitHub/social rendering; [architecture text](../docs/architecture.md) accompanies the diagram.

## Color

| Color | Hex | Role |
| --- | --- | --- |
| Vermilion | `#FF4B00` | README symbol, social panel, and icon background |
| Ink | `#271B16` | Primary mark and lettering |
| Paper | `#F4E9D8` | Inverse mark and warm neutral surfaces |
| Sea glass | `#9CAFA8` | Supporting atmosphere |
| Warm white | `#FCFAF8` | README grid background |
| Rose gray | `#DDD3D5` | README grid lines |

Use ink on vermilion/paper and paper on ink. The README uses a vermilion symbol, near-black text and warm-white grid. Keep grain behind artwork, never inside the mark.

## Typography and spacing

- **Instrument Serif Regular:** wordmark. **Inter Regular:** banner headline/labels. **Space Mono Regular:** technical supporting text. All use SIL OFL 1.1.
- Leave one band-height of clear space. Keep the lockup at least 160px wide; use the four-band mark below 32px.
- Preserve proportions and curves. Do not stretch, outline, shadow or add gradients to the mark. Use inverse assets on dark backgrounds.
- Database marks indicate connector coverage, not endorsement or universal federation. Community artwork must not imply an official SYNEHQ release.

## Reproduce the artwork

On a build host, use Python 3, Pillow 11.3.0, fonttools 4.60.1 and CairoSVG 2.8.2:

```sh
python scripts/render_brand.py \
  --serif /path/to/InstrumentSerif-Regular.ttf \
  --sans /path/to/Inter.ttf \
  --output brand
```

Add `--banner-only` for the banner pair. `--database-marks` overrides the checked-in sprite. Social grain uses a fixed seed.

Render the architecture separately:

```sh
python scripts/render_architecture.py \
  --serif /path/to/InstrumentSerif-Regular.ttf \
  --sans /path/to/Inter.ttf \
  --database-marks brand/database-marks.svg \
  --architecture-marks brand/architecture-marks.svg \
  --output artifacts/architecture
```

The renderer checks label widths and emits SVG, 2× PNG and `architecture-layout.json`. Review them before copying SVG/PNG into `brand/`. Edit layout in `scripts/render_architecture.py` or a vector editor.

Fonts are supplied to the renderer, not bundled:

- [Instrument Serif](https://github.com/google/fonts/blob/main/ofl/instrumentserif/InstrumentSerif-Regular.ttf) · [OFL](https://github.com/google/fonts/blob/main/ofl/instrumentserif/OFL.txt)
- [Inter, pinned regular](https://github.com/google/fonts/blob/0b58fb370093f9a9f4ff785d94405710b79de67c/ofl/inter/Inter%5Bopsz%2Cwght%5D.ttf) · [OFL](https://github.com/google/fonts/blob/0b58fb370093f9a9f4ff785d94405710b79de67c/ofl/inter/OFL.txt)
- [Space Mono](https://github.com/google/fonts/blob/main/ofl/spacemono/SpaceMono-Regular.ttf) · [OFL](https://github.com/google/fonts/blob/main/ofl/spacemono/OFL.txt)

SHA-256 of font files used:

```text
InstrumentSerif-Regular.ttf
498efd461f6ddfcb7a111bf9a565709d2085d48201d501ead960d93e84ffbb88
Inter.ttf
29160a80ff49ddcab2c97711247e08b1fab27a484a329ce8b813d820dc559031
```

Kelvo artwork/renderers use [Apache-2.0](../LICENSE), without trademark rights. Fonts retain OFL terms. Third-party marks retain the licenses and ownership recorded in [database sources](database-marks.SOURCES.md) and [architecture sources](architecture-marks.SOURCES.md).
