# Product marks used in the Kelvo architecture diagram

These marks identify databases, components and compatible consumers in the documented workflow. They remain the property of their respective owners and do not imply endorsement of Kelvo or SYNEHQ. The diagram also reuses the symbols covered by [database mark sources and licenses](database-marks.SOURCES.md).

## Pinned sources

| Symbol ID | Product | Exact upstream SVG | Source SHA-256 |
| --- | --- | --- | --- |
| `googlebigquery` | Google BigQuery | [Simple Icons](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/googlebigquery.svg) | `9911daac258359ac5e73916bc8e97f6df03d5f5845b210bdb99c629f8caf0f84` |
| `apachearrow` | Apache Arrow | [Simple Icons](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/apachearrow.svg) | `ad07fa0b330acdc12dfa8e08973d8902fd06af0f5cbb29f34aad068e120ad825` |
| `python` | Python | [Simple Icons](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/python.svg) | `ad9468e1c4903f73ae7eebfbe980f0f727a10db695be3d914e7d8bd25356a862` |
| `nats` | NATS | [CNCF artwork](https://raw.githubusercontent.com/cncf/artwork/d8ed92555f9aae960ebd04788b788b8e8d65b9f6/projects/nats/icon/color/nats-icon-color.svg) | `aeac64fac2ef4226a921508afc8097bd6300cdc9361779acdd19ec650a961992` |

Simple Icons release `16.30.0`, commit `ad0ef17e0036bf3e87e91c424ec2eac8cf950029`, is distributed under [CC0-1.0](https://github.com/simple-icons/simple-icons/blob/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/LICENSE.md). Its distribution license does not grant trademark rights.

NATS artwork is pinned to CNCF artwork commit `d8ed92555f9aae960ebd04788b788b8e8d65b9f6`. The [artwork terms](https://github.com/cncf/artwork/blob/d8ed92555f9aae960ebd04788b788b8e8d65b9f6/LICENSE.md) refer to the [Linux Foundation trademark usage guidelines](https://www.linuxfoundation.org/legal/trademark-usage). It is not relicensed under Kelvo's Apache-2.0 license.

## Composition

Original paths, proportions and viewBoxes are preserved. Each SVG wrapper becomes a named `symbol` in `architecture-marks.svg`. The diagram embeds those symbols so it needs no external resources. NATS retains its original colors. The Simple Icons paths use their pinned metadata colors: BigQuery `#669DF6`, Apache Arrow `#000000`, and Python `#3776AB`.

Python identifies the documented Python/PyArrow consumption path. It does not imply a built-in notebook service. NATS identifies optional cluster dispatch and state, while Apache Arrow identifies the output format. Database marks illustrate native connector coverage; they do not imply universal federation. Generic application, chart, file and job icons are original diagram elements.

## Product references

- [Google Cloud product icons](https://cloud.google.com/icons)
- [Apache Arrow visual identity](https://arrow.apache.org/visual_identity/) and [Apache trademark policy](https://www.apache.org/foundation/marks/)
- [Python logos](https://www.python.org/community/logos/) and [PSF trademark policy](https://www.python.org/psf/trademarks/)
- [CNCF artwork](https://github.com/cncf/artwork)
