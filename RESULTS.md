# Results

## Rate benchmark, production-style: 90 s holds, 1 GB limit (2026-11-24)

Same capture and replay seed for every collector: `--rates 5,10,20 --duration 120 --decision-wait 90s --memory 1g --seed 1`. The plain tail samplers hold 90 s; `routed_tail_sampling` keeps its 45/60/90 s buckets. Raw: `bench/results.json`, log `bench/benchmark_rates_90s.log`. The earlier 5 s-wait run is archived in `archive/bench_before_new_methods/`.

| rate | collector                  | peak MB | OOM | spans kept | evidence |
| ---- | -------------------------- | ------- | --- | ---------- | -------- |
| 5/s  | span_filter                | 110     | no  | 14.8%      | 100.0%   |
| 10/s | span_filter                | 117     | no  | 14.9%      | 100.0%   |
| 20/s | span_filter                | 111     | no  | 15.0%      | 100.0%   |
| 5/s  | routed_tail_sampling       | 698     | no  | 7.9%       | 27.3%    |
| 10/s | routed_tail_sampling       | 1059    | yes | 1.1%       | 4.2%     |
| 20/s | routed_tail_sampling       | 1015    | yes | 0.0%       | 0.0%     |
| 5/s  | anomalies_sampling         | 1037    | no  | 100.0%     | 100.0%   |
| 10/s | anomalies_sampling         | 1047    | yes | 0.0%       | 0.0%     |
| 20/s | anomalies_sampling         | 1063    | yes | 0.0%       | 0.0%     |
| 5/s  | anomalies_sampling_limited | 185     | no  | 4.5%       | 3.2%     |
| 10/s | anomalies_sampling_limited | 194     | no  | 2.3%       | 3.5%     |
| 20/s | anomalies_sampling_limited | 189     | no  | 1.1%       | 1.6%     |

- With equal holds and limits, `span_filter` is the only method that stays flat (110-117 MB), keeps 100% evidence and reduces to about 15% of spans, at every rate.
- `anomalies_sampling` hits the 1 GB limit already at 5/s (1037 MB, no OOM but at the limit) and is OOM-killed at 10/s and 20/s, losing everything it held (0% kept).
- `routed_tail_sampling` uses less memory than `anomalies_sampling` at 5/s (698 vs 1037 MB), its per-job holds do shorten the wait, but is also OOM-killed from 10/s, and keeps only 27% evidence at 5/s.
- `anomalies_sampling_limited` survives only because its memory limiter refuses data (evidence 1.6-3.5%).
- Evidence is in-sample for the thresholds (replayed seed-42 capture). Memory and reduction are the fair comparison.

## Rate benchmark: 5 s holds, no memory limit (2026-11-24)

`scripts/benchmark_rates.py --rates 5,10,20,40 --duration 30` on `captures/supermarket_seed42`, each collector alone in Docker (no memory limit), tail samplers with 5 s `decision_wait` except `routed_tail_sampling` (calibrated 45/60/90 s). Raw: `bench/results.json`. Replayed traces carry the seed-42 data the thresholds were calibrated on, so evidence is in-sample.

| rate | collector                  | peak MB | spans kept | evidence |
| ---- | -------------------------- | ------- | ---------- | -------- |
| 5/s  | span_filter                | 104     | 14.8%      | 100.0%   |
| 10/s | span_filter                | 110     | 14.5%      | 100.0%   |
| 20/s | span_filter                | 113     | 14.8%      | 100.0%   |
| 40/s | span_filter                | 124     | 14.9%      | 100.0%   |
| 5/s  | routed_tail_sampling       | 245     | 1.1%       | 0.0%     |
| 10/s | routed_tail_sampling       | 443     | 7.9%       | 27.6%    |
| 20/s | routed_tail_sampling       | 847     | 7.9%       | 27.3%    |
| 40/s | routed_tail_sampling       | 1557    | 8.2%       | 26.9%    |
| 5/s  | anomalies_sampling         | 159     | 100.0%     | 100.0%   |
| 10/s | anomalies_sampling         | 262     | 100.0%     | 100.0%   |
| 20/s | anomalies_sampling         | 392     | 100.0%     | 100.0%   |
| 40/s | anomalies_sampling         | 1079    | 100.0%     | 100.0%   |
| 40/s | baseline_naive             | 191     | 100.0%     | 100.0%   |
| 40/s | sampling                   | 108     | 9.9%       | 10.6%    |

- `span_filter` memory stays flat (104 to 124 MB, close to the 40/s baseline's 191 MB) while keeping 100% evidence at about 15% of spans. No OOM at any rate.
- `routed_tail_sampling` memory grows with rate, from 245 MB at 5/s to 1.5 GB at 40/s, and keeps under 30% evidence. It is both the most expensive and the least accurate of the new methods here.
- `anomalies_sampling` keeps everything (the 1 s latency policy matches every trace), so its memory (1 GB at 40/s) buys no reduction.
- The 5/s `routed_tail_sampling` row is unreliable: the replay got a `DEADLINE_EXCEEDED` and achieved only 2.4 traces/s. Do not read its 0% evidence as a result.
- The `anomalies_sampling_limited` rows are not stable between rates (97% kept at 5/s, 2.5% at 40/s), as in the earlier runs.
- App-side methods (`job_buffer`, `resolution`) are not covered: the replay bypasses the app.

## Run 3: all methods, seed 8 (2026-11-24)

Same setup as run 2 but seed 8, and a 200 s wait after the run so every hold (up to 90 s) flushes. Raw scores: `out/eval_seed8.txt`.

| method                     | traces | spans  | bytes  | evidence | anomalous | error  | slow   | normal |
| -------------------------- | ------ | ------ | ------ | -------- | --------- | ------ | ------ | ------ |
| anomalies_sampling         | 100.0% | 100.0% | 100.0% | 100.0%   | 100.0%    | 100.0% | 100.0% | 100.0% |
| anomalies_sampling_limited | 37.6%  | 38.3%  | 38.7%  | 44.6%    | 44.6%     | 39.6%  | 100.0% | 35.1%  |
| sampling                   | 10.1%  | 10.3%  | 10.9%  | 14.5%    | 14.5%     | 7.5%   | 0.0%   | 8.8%   |
| span_filter                | 100.0% | 15.0%  | 14.0%  | 100.0%   | 100.0%    | 100.0% | 100.0% | 100.0% |
| routed_tail_sampling       | 32.2%  | 37.0%  | 37.9%  | 47.0%    | 47.0%     | 100.0% | 76.9%  | 13.5%  |
| app_job_buffer             | 67.1%  | 40.6%  | 41.4%  | 100.0%   | 100.0%    | 100.0% | 100.0% | 42.6%  |
| app_resolution             | 100.0% | 14.5%  | 13.2%  | 100.0%   | 100.0%    | 100.0% | 100.0% | 100.0% |

Findings:
- `span_filter` and `app_resolution` replicate on a second unseen seed: about 15% of spans, 100% evidence.
- `app_job_buffer` keeps 100% evidence but 41% of spans this time (29% in run 2).
- `routed_tail_sampling` again keeps only 47% evidence even with a full flush, so the shortfall in run 2 was not just an incomplete flush. Trace-by-trace check (seed 8): all 44 missed anomalous traces were single-job traces, fully routed (no unrouted spans), and their whole-trace duration was below their job's slow threshold. Whole-trace duration barely separates anomalous from normal traces: comparing each trace to its job's threshold, anomalous traces have a median of 78% of the threshold and normal traces 70%. Halving the threshold would catch 100% of anomalous traces but keep 95% of normal traces, and keeping 94% of anomalies needs 60% of the threshold, which keeps 75% of normal traces. The collector's `tail_sampling` latency policy only sees whole-trace duration, so this method cannot reach the span-level methods' reduction at the same evidence. The span-level methods work because the injected slow span stands out on its own even when the job as a whole is long.
- `anomalies_sampling_limited` behaves differently per run (0% evidence in run 2, 45% here), so it depends on memory-limiter timing and is not a stable baseline.

## Run 2: all methods, seed 7 (2026-11-24)

`mock-io-run --scenario scenarios/supermarket.yaml --latency-scale 1 --rate 0.5 --runs 260 --workers 8 --seed 7`, all eight collectors fed at once, app-side reducers `job_buffer,resolution` enabled. Thresholds were calibrated on the seed-42 capture, so this is out-of-sample. Raw scores: `out/eval_seed7.txt`.

| method                     | traces | spans  | bytes  | evidence | anomalous | error  | slow  | normal | email_outage | huge_blob | slow_price_engine | slow_tax_service | slow_weather_api |
| -------------------------- | ------ | ------ | ------ | -------- | --------- | ------ | ----- | ------ | ------------ | --------- | ----------------- | ---------------- | ---------------- |
| anomalies_sampling         | 100.0% | 100.0% | 100.0% | 100.0%   | 100.0%    | 100.0% | 100.0%| 100.0% | 100.0%       | 100.0%    | 100.0%            | 100.0%           | 100.0%           |
| anomalies_sampling_limited | 11.5%  | 11.7%  | 11.6%  | 0.0%     | 0.0%      | 0.0%   | 7.1%  | 14.6%  | 0.0%         | 0.0%      | 0.0%              | 0.0%             | 0.0%             |
| sampling                   | 11.9%  | 13.4%  | 14.1%  | 12.8%    | 12.8%     | 6.5%   | 21.4% | 11.6%  | 4.5%         | 20.0%     | 18.2%             | 0.0%             | 33.3%            |
| span_filter                | 100.0% | 14.6%  | 13.0%  | 100.0%   | 100.0%    | 100.0% | 100.0%| 100.0% | 100.0%       | 100.0%    | 100.0%            | 100.0%           | 100.0%           |
| routed_tail_sampling       | 12.3%  | 10.2%  | 10.9%  | 46.8%    | 46.8%     | 100.0% | 7.1%  | 0.5%   | 100.0%       | 30.0%     | 0.0%              | 0.0%             | 0.0%             |
| app_job_buffer             | 45.0%  | 28.6%  | 29.2%  | 100.0%   | 100.0%    | 100.0% | 92.9% | 28.6%  | 100.0%       | 100.0%    | 100.0%            | 100.0%           | 100.0%           |
| app_resolution             | 100.0% | 14.0%  | 12.2%  | 100.0%   | 100.0%    | 100.0% | 100.0%| 100.0% | 100.0%       | 100.0%    | 100.0%            | 100.0%           | 100.0%           |

traces/spans/bytes: share kept of the reference; evidence: anomalous traces with at least one injected-anomaly span kept; other columns: recall of that trace type (any span kept).

Notes:
- `span_filter` and `app_resolution` keep about 14% of spans with 100% evidence out of sample. `span_filter` needs no app changes.
- `app_job_buffer` keeps 100% evidence but about twice as many spans (28.6%).
- `routed_tail_sampling` missed all slow-service anomalies. The run was only given about 100 s to flush, while holds go up to 90 s, so its output may be incomplete. Run 3 reran it with a full flush and the shortfall remained.
- `anomalies_sampling_limited` produced almost no output until late in the run (memory limiter), and kept no evidence.
- Span-level methods keep a skeleton of every trace, so their trace recall is near 100% by construction; use the evidence column.
- Old results are archived in `archive/2026-11-24_pre_new_methods/`.

## Run 1: earlier baseline comparison (seed 42 capture)

| method             | traces | bytes | anomalous | error  | slow  | email_outage | huge_blob | slow_price_engine | slow_tax_service | slow_weather_api |
| ------------------ | ------ | ----- | --------- | ------ | ----- | ------------ | --------- | ----------------- | ---------------- | ---------------- |
| anomalies_sampling | 95.6%  | 95.9% | 97.8%     | 100.0% | 96.3% | 100.0%       | 97.6%     | 95.5%             | 93.8%            | 100.0%           |
| sampling           | 7.1%   | 7.6%  | 6.5%      | 6.5%   | 3.7%  | 7.1%         | 7.1%      | 9.1%              | 0.0%             | 6.2%             |
