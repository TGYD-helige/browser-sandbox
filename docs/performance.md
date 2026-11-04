# Performance acceptance

Browser integration records workloads on native AMD64 and ARM64 runners with a 2 CPU, 2 GiB container, three warmups, and 20 measured samples per workload. Results include environment metadata, raw measurements, sample counts, median, and P95. Local HTTP storage deliberately excludes production object-store and network latency.

## Workloads and phases

The image benchmark covers an unused remotely configured execution, a temporary browser, and small/8 MiB IndexedDB remote Profiles. The unused workload asserts that the browser stays unused and that the storage adapter receives zero load/save/commit calls. It records registration and finalization costs without starting Chrome. Used workloads verify real rendering, screenshots, and restored data.

Registration, first browser readiness, and finalization are measured separately. Runtime preparation records Profile acquisition (including exclusive access and permission preparation), archive source lookup/restore, and Browserless launch (including queue wait) separately. First readiness also includes client startup, navigation, and screenshot; finalization includes checkpoint, process exit, archive/encryption, upload, and commit. Archive HTTP transfer timing is also reported. Phase values are not interchangeable and should not be added as independent wall-clock totals.

The optional process-local `browser-sandbox.preparation` diagnostics channel emits one record per successful preparation, containing executionId and three duration values. Production retains no timing records. Image verification alone preloads the test observer, which writes private temporary records and removes measured records after collection. Timing records contain no capability endpoints, Profile identities, credentials, or archive grants.

The unused harness job runs the actual published Pi CLI with a scripted local provider and no browser tool calls. It alternates ordinary and managed configurations for each matched sample, confirms the real task completes, and checks that the execution remains unused. Its startup measurement includes the complete short task and CLI exit. Image results provide the separate host registration/finalization measurements. This is a representative Pi measurement, not evidence about every harness or real-model latency; DSH and OpenCode startup performance remain unmeasured.

## Baseline comparison and regression budget

For PRs, `benchmark-pair.sh` builds the target commit as the baseline and runs the same measurement client against baseline and candidate images sequentially on the same native runner. Each before/after sample uses the same workload and sample index; the two complete image runs are not interleaved. Master pushes use the previous commit, and manual dispatch accepts a baseline commit. If a historical commit has no Dockerfile, only the current report is produced and no comparison is claimed.

Both runs use the current benchmark client without changing the baseline runtime. Older runtimes without preparation diagnostics omit those phase metrics explicitly; comparison covers every phase available in the baseline and requires matching workload, environment, architecture, browser version, and sample count. New preparation phases are reported for the candidate without inventing historical values.

The initial CI regression budget is `candidate P95 <= baseline P95 * 1.25 + 25 ms` for each shared workload phase. The relative allowance and absolute floor absorb runner jitter and small filesystem costs; this is a provisional regression guard, not an agreed production latency SLA. The comparison also reports paired median/P95 sample deltas. A failed budget retains the reports for investigation rather than silently widening the budget. Upstream/browser upgrades require a separately reviewed comparable baseline; mismatched versions fail comparison explicitly.

Artifacts are `browser-performance-<runner>` (baseline, current, and comparison JSON) and `unused-harness-performance` (ordinary/managed Pi and paired overhead). To collect a current image report locally, set `BENCHMARK_OUTPUT=/tmp/browser-performance.json` when running `verify-image.sh`. For a paired run, build `browser-sandbox:local`, then run `bash .github/scripts/benchmark-pair.sh <baseline-commit> /tmp/browser-performance-pair` with a usable Docker daemon and network access.

Production/cloud latency, production workload sizes, slow-disk behavior, and DSH/OpenCode startup budgets still require deployment-specific measurement. Do not infer an optimization speedup from a single report or from the absence of storage calls.
