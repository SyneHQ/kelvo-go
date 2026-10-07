# Kelvo Go contributor instructions

Kelvo Go is an independent open-source analytics gateway by SYNEHQ.
- Keep DuckDB/Arrow/native drivers behind explicit interfaces. No DataFusion, Drill, Hakopod, Kubernetes or model/GPU runtime in the initial dependency set.
- Correctness before speed: preserve NULLs, decimals, integer widths and timestamps; fail explicitly on unsupported behavior.
- Stream borrowed Arrow batches synchronously; never retain a batch beyond its owner without Retain/Release. Keep DuckDB raw connection lifetimes inside sql.Conn.Raw.
- The pinned DuckDB Go path materializes execution before Arrow delivery. Do not describe it as streaming query execution or assume delivery backpressure bounds native query memory.
- No credentials/DSNs/private project files in source, fixtures, logs or commits. Config refers to environment variable names.
- The default branch is cargo. No branch prefixes named after models or assistants. Use feature/, fix/, docs/ for topic branches.
- Close pull requests with rebase merging only. Do not create merge commits or squash the focused commit history.
- Build and test in an isolated Linux environment. Follow the task's host and resource restrictions; preserve unrelated services and work.
- Keep ownership boundaries assigned by the integration lead. Do not change shared contracts without coordination.
- Any production/performance claim requires real measurements. Document prototype limitations and failed tests.
