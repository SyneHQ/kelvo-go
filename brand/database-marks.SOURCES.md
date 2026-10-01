# Database marks used in the Kelvo banner

These third-party marks identify supported database products. Product names and marks belong to their respective owners; their inclusion does not imply endorsement of Kelvo or SYNEHQ. Source artwork is vector-only and is embedded in the banner so it does not request external resources.

## Pinned sources

- Devicon: commit `7330accdbc47e2dc0c19789a48533c4a3c50fe58`. The three existing local SYNEHQ website marks were verified byte-for-byte against this commit. License: MIT (full notice below).
- Simple Icons: release `16.30.0`, commit `ad0ef17e0036bf3e87e91c424ec2eac8cf950029`. The existing local ClickHouse mark was verified byte-for-byte against this commit. The other four marks were retrieved at this same commit. License: [CC0-1.0](https://github.com/simple-icons/simple-icons/blob/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/LICENSE.md).

| Symbol ID | Product | Exact upstream SVG | Source SHA-256 |
| --- | --- | --- | --- |
| `postgresql` | PostgreSQL | [Source](https://raw.githubusercontent.com/devicons/devicon/7330accdbc47e2dc0c19789a48533c4a3c50fe58/icons/postgresql/postgresql-original.svg) | `f220a436258ed014c512ba4f9c0de6b9e5c2c4b55331a0a29f8c7a1a12c36443` |
| `mysql` | MySQL | [Source](https://raw.githubusercontent.com/devicons/devicon/7330accdbc47e2dc0c19789a48533c4a3c50fe58/icons/mysql/mysql-original.svg) | `f73fa5d6b9da33fbf540ff96f5360c24b6d29d883ecfe6fcaecf89a8d3456067` |
| `mongodb` | MongoDB | [Source](https://raw.githubusercontent.com/devicons/devicon/7330accdbc47e2dc0c19789a48533c4a3c50fe58/icons/mongodb/mongodb-original.svg) | `d9f2bf7041264f4747e9969fb82ede0199c0ac97dfa8632aa0687d702ec05f7b` |
| `clickhouse` | ClickHouse | [Source](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/clickhouse.svg) | `9fa7c12613763111ca26bbc1fe10994960028bc3da9faf47247de231a0b27589` |
| `duckdb` | DuckDB | [Source](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/duckdb.svg) | `2d53f8978d58ac12411fc618569a492922bb15e3e1798b56ba1511e72c62f826` |
| `snowflake` | Snowflake | [Source](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/snowflake.svg) | `75690c4c666a406278286284f89896e8ef55dd0bfbc791acb910ce40e4a46aa9` |
| `databricks` | Databricks | [Source](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/databricks.svg) | `454aa3efdb318c655e3da2ea844e3577a9c47a20e24d20fff6489362176e5fe4` |
| `elasticsearch` | Elasticsearch | [Source](https://raw.githubusercontent.com/simple-icons/simple-icons/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/icons/elasticsearch.svg) | `f866863bb80c3f1e22cb49541442c9db2112bba74b8c1210328bd3536984e6a3` |

## Composition changes

- Every original path and viewBox is preserved. The SVG wrappers are converted to reusable `symbol` elements and a product title is added.
- PostgreSQL, MySQL, and MongoDB retain their source fills.
- Simple Icons paths have no source fill. The sprite sets Snowflake to `#29B5E8`, Databricks to `#FF3621`, and Elasticsearch to `#005571`, matching their pinned Simple Icons metadata. ClickHouse and DuckDB use ink `#111110` on the banner's neutral background.
- The banner supplies neutral tile backgrounds, repeats some marks, and fades secondary instances. No symbol geometry is changed.

## Upstream brand references

- PostgreSQL: https://www.postgresql.org/about/press/presskit/
- MySQL: https://www.mysql.com/about/legal/logos.html
- MongoDB: https://www.mongodb.com/company/newsroom/brand-resources
- ClickHouse: https://github.com/ClickHouse/ClickHouse/blob/12bd453a43819176d25ecf247033f6cb1af54beb/website/images/logo-clickhouse.svg
- DuckDB: https://duckdb.org/
- Snowflake: https://www.snowflake.com/brand-guidelines/
- Databricks: https://brand.databricks.com/Styleguide/Guide/
- Elasticsearch: https://www.elastic.co/brand

Brand references for the Simple Icons marks are recorded in the pinned package [metadata](https://github.com/simple-icons/simple-icons/blob/ad0ef17e0036bf3e87e91c424ec2eac8cf950029/data/simple-icons.json). Asset licenses do not grant trademark rights.

## Devicon MIT license

Upstream: https://github.com/devicons/devicon/blob/7330accdbc47e2dc0c19789a48533c4a3c50fe58/LICENSE

```text
The MIT License (MIT)

Copyright (c) 2015 konpa

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
