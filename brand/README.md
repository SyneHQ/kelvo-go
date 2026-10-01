# Kelvo identity

![Kelvo — open-source analytics by SYNEHQ.](kelvo-banner.png)

An editorial identity for a technical product: a small serif lockup, an abstract circular symbol, vermilion, and a soft mineral-and-peach field. Six bands alternate between two curved fields inside a circle. The interleaving suggests data brought together; the mark is geometric, not a letter or monogram.

The product name is **Kelvo**. The organization is **SYNEHQ**. Use **Kelvo by SYNEHQ** when both appear together. The uppercase wordmark is artwork; use normal title case in prose.

## Assets

| Asset | Use |
| --- | --- |
| [Primary mark](kelvo-mark.svg) · [Inverse mark](kelvo-mark-inverse.svg) | Six-band circular symbol, transparent vector, 32 px and larger |
| [Small mark](kelvo-mark-small.svg) | Four-band optical variant for 16–31 px; recolor to paper on dark surfaces |
| [Wordmark](kelvo-wordmark.svg) · [Inverse wordmark](kelvo-wordmark-inverse.svg) | Horizontal symbol and outlined serif lockup |
| [Banner SVG](kelvo-banner.svg) · [Banner PNG](kelvo-banner.png) | 1600 × 720 README and wide-format artwork |
| [Social SVG](kelvo-social.svg) · [Social PNG](kelvo-social.png) | 1600 × 840 master; 1200 × 630 export |
| [Square icon](kelvo-icon.png) | 512 × 512 ink mark on vermilion |

SVG lettering and marks are paths: no installed fonts or network requests are needed to display them. Banner SVGs embed their grain field as a PNG; lettering and mark remain editable vectors. PNG exports provide consistent rendering in GitHub and social clients.

## Color

| Color | Hex | Role |
| --- | --- | --- |
| Vermilion | `#FF4B00` | Signature panel and icon background |
| Ink | `#271B16` | Primary mark and lettering |
| Paper | `#F4E9D8` | Inverse mark and warm neutral surfaces |
| Sea glass | `#9CAFA8` | Supporting atmosphere |

Use ink on vermilion or paper. Use paper on ink. Keep grain behind artwork, never inside the standalone mark. The banner's atmosphere is original procedural artwork, not a photograph or an extracted reference asset.

## Typography and spacing

**Instrument Serif Regular** supplies the wordmark. **Space Mono Regular** is the supporting typeface for technical content outside the logo artwork. Both are SIL Open Font License 1.1 fonts. Keep the serif at its native width; do not stretch or artificially condense it.

Keep at least one band height of clear space around the visible mark. Keep the lockup at least 160 px wide. Below 32 px, use the four-band small mark to preserve the curved fields and negative space. Banner lockups are deliberately small and centered, with generous space; do not enlarge the wordmark to fill the textured panel. Keep banners free of extra taglines and technical labels.

Preserve proportions, band count, and the supplied curves. Use the inverse assets on dark surfaces. Avoid outlines, drop shadows, extra colors, or gradients applied to the mark itself. Do not imply that a community project is an official SYNEHQ release.

## Reproduce the artwork

Run the renderer on a build host with Python 3, Pillow 11.3.0, fonttools 4.60.1, and CairoSVG 2.8.2. It uses a fixed grain seed and generates all ten assets.

```sh
python scripts/render_brand.py \
  --serif /path/to/InstrumentSerif-Regular.ttf \
  --output brand
```

Instrument Serif is supplied to the renderer; font binaries are not bundled in this repository. Upstream typeface sources and licenses:

- [Instrument Serif Regular](https://github.com/google/fonts/blob/main/ofl/instrumentserif/InstrumentSerif-Regular.ttf) · [OFL](https://github.com/google/fonts/blob/main/ofl/instrumentserif/OFL.txt)
- [Space Mono Regular](https://github.com/google/fonts/blob/main/ofl/spacemono/SpaceMono-Regular.ttf) · [OFL](https://github.com/google/fonts/blob/main/ofl/spacemono/OFL.txt)

SHA-256 of the Instrument Serif font used for these exports:

```text
InstrumentSerif-Regular.ttf
498efd461f6ddfcb7a111bf9a565709d2085d48201d501ead960d93e84ffbb88
```

Kelvo artwork and the renderer are included under the repository's [Apache-2.0 license](../LICENSE). That license does not grant trademark rights. Font software remains under its upstream OFL terms.
