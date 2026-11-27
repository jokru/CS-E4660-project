# CS-E4660: Investigating tracing

> This README is the research report: questions, approach, results, lessons and future work. [PROJECT.md](PROJECT.md) holds rougher working notes kept during the project (ideas, intermediate results and decisions), [RESULTS.md](RESULTS.md) the full result tables, and [RUNNING.md](RUNNING.md) the setup and running instructions for the test bench.

# 1. Research Questions

- Is it worth it to go deeper than OTel collector config options for trace processing
- Do simple reducers miss rare exceptions?
- Does a smarter reducer matter enough for finding exceptions and anomalies?
- Application level vs. collector level adaptability?

# 2. Approach

The project compares trace reduction methods on one realistic, repeatable workload, scoring each by how much it keeps against whether it keeps what matters: the traces with errors and anomalies.

## Case study

Production traces have no ground truth: nobody knows which traces held the problem an operator would have needed. So the workload comes from a mock data platform, the [Mock I/O app](RUNNING.md#case-study-app-mock-io-app), built on Dagster. The [supermarket scenario](RUNNING.md#supermarket-scenario) has 49 jobs (importers, transformers, loaders) calling databases, cloud storage, HTTP APIs, and first- and third-party services, which gives traces of about 900 spans across many services. Each job runs in its own process, and runs overlap.

Problems are injected as labelled [anomalies](RUNNING.md#anomalies): rare slow service calls, huge uploads, and outage windows, on top of a low background failure rate. Every injected anomaly is logged by trace and span ID, independently of what the collectors keep, so each method can be scored against what really happened. Outcomes are fixed by a seed, so the same workload can be replayed.

## Methods

The methods are grouped by where the reduction logic runs and how much it holds in memory, since that is what the research questions ask about:

| Group                                | Methods                                                                    | Holds                             |
| ------------------------------------ | -------------------------------------------------------------------------- | --------------------------------- |
| Reference                            | `baseline_naive`                                                           | nothing, keeps everything         |
| Stock collector, stateless           | `sampling` (10% of traces)                                                 | nothing                           |
| Stock collector, tail sampling       | `anomalies_sampling`, `anomalies_sampling_limited`, `routed_tail_sampling` | whole traces until a decision     |
| Collector with calibrated thresholds | `span_filter`                                                              | nothing                           |
| Application side                     | `job_buffer`, `span_filter_path`                                                 | a running job's spans, or nothing |
| LLM agent                            | did not have time                                                          |                                   |

The stock collector methods use only OpenTelemetry Collector configuration. `span_filter` and `routed_tail_sampling` are still plain configuration, but generated from thresholds learned from a recorded baseline run (5× the median duration per service and span name, and per-job run durations). This separates what the collector can do from where its parameters come from. The application-side reducers run inside the app, where they know when a job has finished and need not guess how long to wait. All methods decide on observable signals only (errors and durations), never on the injected anomaly labels.

The method list grew from the results. The first experiments used the stock methods; tail sampling ran out of memory as the trace rate grew, so the later methods were designed to hold less or nothing ([Span-level and app-side methods](RUNNING.md#span-level-and-app-side-methods)).

## Metrics

- **Reduction:** the share of traces, spans and bytes kept relative to the baseline.
- **Evidence:** the share of anomalous traces of which at least one span with an injected anomaly was kept. This is the main accuracy measure, because span-level methods keep a skeleton of every trace, so trace-level recall is near 100% for them by construction.
- **Recall by trace type:** anomalous, error, slow (at or above the baseline's 95th percentile duration) and normal traces, and per anomaly label. Recall on normal traces shows how much a method keeps without reason.
- **Cost:** peak collector memory, whether the collector was killed for running out of memory, and requests it refused, at increasing trace rates.

## Experimental design

- **Accuracy:** one app workload (260 runs, 10 iterations of the 26 leaf jobs, Poisson arrivals at 0.5 runs/s with up to 8 at once, real latencies) is fed to every collector at once, merged into one OpenTelemetry Collector, with the app-side reducers sending their streams in parallel. Trace IDs differ between runs, so this is the only way to compare methods on the same traces.
- **Out of sample:** thresholds are calibrated on seed 42 and evaluated on seeds 7 and 8.
- **Scale:** the app tops out at a few runs per second (Dagster's process startup), so a [replay tool](RUNNING.md#benchmark-high-trace-rates) resends a recorded capture at 5 to 40 traces/s, with fresh IDs and matching ground truth, to each collector alone. App-side reducers cannot be measured this way, since the replay bypasses the app.

# 3. How

The test bench is written in Python (3.12, managed with uv): the app on Dagster with the OpenTelemetry SDK, and the methods as OpenTelemetry Collector (contrib) configs run in Docker. Commands are in the [Command reference](RUNNING.md#command-reference).

```mermaid
flowchart LR
  cal[(seed 42 capture)] --> calib[calibrate_thresholds.py]
  calib --> thr[(thresholds and generated configs)]
  thr --> app
  thr --> col
  app[Mock I/O app] -->|all spans, port 4317| col[OpenTelemetry Collector:<br/>one pipeline per method]
  app -->|job_buffer and span_filter_path streams,<br/>ports 4319 and 4320| col
  replay[mock-io-replay] -.->|recorded capture,<br/>5-40 traces/s| col
  app --> gt[(ground truth)]
  col --> out[(one output file per method)]
  out --> eval[mock-io-eval]
  gt --> eval
```

## Calibration

`scripts/calibrate_thresholds.py` reads a `baseline_naive` capture of seed 42 and computes a latency threshold per service and span name (5× the median, for keys with at least 20 spans) and each job's run durations. It writes `thresholds/supermarket.json`, which the app-side reducers read, and generates the `span_filter` and `routed_tail_sampling` collector configs from it.

## Accuracy runs

1. Start one collector container with every method's config merged in. Each method has its own pipeline and output file, so they all see the same spans.
2. Run the app workload with a new seed (7 or 8), the ground truth log and the app-side reducers enabled. The app sends every span to the collector's main port, and the reducers send their reduced streams to their own ports, so the full export is unaffected.
3. Wait until the longest tail sampling hold has passed (200 s in run 3, for holds up to 90 s), then stop the collector so the last batches are written.
4. Score every output against the baseline output and the ground truth with `mock-io-eval`, which prints the recall matrix.

## Replay benchmark

The app cannot produce the trace rates where the methods differ in cost. Every run goes through Dagster, which takes seconds to start the job processes, so the app tops out at a few runs per second, while tail sampling's memory only becomes a problem from about 10 traces/s (killed at a 1 GB limit with a 60 s decision wait). Without a faster load generator the memory results could not have been measured.

`mock-io-replay` therefore skips the app and resends a recorded capture over OTLP at a target rate. Each copy of a trace gets new trace and span IDs and current timestamps, while its structure, durations, errors and anomaly labels are kept, and the ground truth is rewritten under the new IDs so the output can be scored as usual. With a fixed seed, every replay sends identical traces, so each collector runs alone (its memory measured with `docker stats`) and is scored against one baseline replay. `scripts/benchmark_rates.py` automates this for every collector at 5, 10, 20 and 40 traces/s.

The replay has caveats that limit what it can answer:

- **Application-side methods are emulated, and their in-app cost is unmeasured.** `job_buffer` and `span_filter_path` run inside the app processes, which the replay bypasses. `mock-io-replay --reducer` therefore applies their keep/drop rules to each replayed trace (`replay_reducers.py`) and sends only what the app would forward. On the seed 8 capture the emulation keeps exactly the same spans as the real processors (31,920 for `span_filter_path` and 89,487 for `job_buffer`, of 220,354), with one approximation: a `job.*` span is treated as having a remote parent. The benchmark measures reduction, evidence and the receiving collector's memory, but not the CPU and memory the reducers cost inside the app.
- **Traces arrive whole.** A replayed trace is sent in one request, while app spans arrive over the length of a run, so the plain tail samplers use a short decision wait (5 s in the first benchmark) instead of the 120 s needed for app runs. A second benchmark used 90 s holds for every tail sampler (plus 1 GB memory limit), which is the comparable setting for memory.
- **In-sample thresholds.** The replayed seed 42 capture is the one the thresholds were calibrated on, so its evidence is optimistic. A replay of the seed 8 capture, which the thresholds never saw, gave the same results for the methods that hold nothing. The accuracy runs are out of sample as well.

# 4. result

Initial hypotheses:

- **Naive sampling misses errors at its sampling rate.** Confirmed on average: `sampling` keeps about 10% of every trace type, anomalous or not. Rare anomaly types, with only a handful of traces each, scatter widely around that rate, and some are missed entirely (`slow_tax_service`: 0% in runs 1 and 2). Relates to "Do simple reducers miss rare exceptions?".
- **Tail sampling keeps anomalies, but its memory grows with the trace rate until it runs out.** Confirmed: `anomalies_sampling` keeps 100% evidence at low rates, but holds every trace until its decision, and was killed at 10 traces/s with a 1 GB limit, losing everything. Its fixed 1 s latency policy also matched almost every trace, so it barely reduced anything. Relates to "Does a smarter reducer matter enough?".


## Findings

All numbers are in `RESULTS.md`. Evidence is the share of anomalous traces of which an injected-anomaly span was kept.

| Method                     | Spans kept | Evidence     | Memory and cost                                                     |
| -------------------------- | ---------- | ------------ | ------------------------------------------------------------------- |
| `sampling` (10%)           | 10-13%     | 13-15%       | flat (108 MB at 40 traces/s)                                        |
| `anomalies_sampling`       | 100%       | 100%         | 1 GB at 5 traces/s with 90 s holds; killed at 10/s                  |
| `anomalies_sampling_limited` | 1-38%    | 0-97%        | survives by refusing data; unstable between runs                    |
| `routed_tail_sampling`     | 8-37%      | 47%          | 698 MB at 5/s; killed at 10/s                                       |
| `span_filter`              | 15%        | 100%         | flat, 104-125 MB from 5 to 40/s                                     |
| `app_span_filter_path`           | 14%        | 100%         | flat, collector side 62-90 MB; in-app cost not measured             |
| `app_job_buffer`           | 24-41%     | 100%         | flat on the collector; reduction depends on the capture and drift   |

- **Calibrated span-level methods win.** `span_filter` and `app_span_filter_path` keep 100% of the evidence at about 15% of the spans, on two unseen seeds (7 and 8), in replays at 5 to 40 traces/s, and on a capture the thresholds had never seen. Random sampling keeps 13-15% of the evidence at a similar volume.
- **Tail sampling is a dead end here, for two reasons.** Its memory grows with rate times hold time, so with realistic 90 s holds both tail samplers are killed at 10 traces/s. And whole-trace latency cannot find the anomalies: anomalous traces reach a median 78% of their job's slow threshold and normal traces 70%, so any cutoff that finds the anomalies also keeps nearly every normal trace. `routed_tail_sampling` (per-job holds of 45 to 90 s) saves some memory but kept only 47% of the evidence and missed most slow-service anomalies.
- **Latency drift.** With latencies scaled to 0.5x, evidence slips to 98-99%; at 2x it stays 100%, but `app_job_buffer` keeps 92% of the spans, so its reduction disappears. The span-level methods grew only to 15-17%. Recalibrating on a recent capture handles both.
- **Not tested.** The in-app CPU and memory cost of the reducers, rare anomaly rates of about 1% or less, and a real production workload.

Answers to the research questions:

- Is it worth it to go deeper than OTel collector config options for trace processing?
- - Partly. The best collector-side method, `span_filter`, is still plain collector configuration, but generated from thresholds learned from a baseline capture. The stock tail sampling configurations were the ones that failed. What pays off is calibrated, span-level decisions, not custom code.
- Do simple reducers miss rare exceptions?
- - Yes
- Does a smarter reducer matter enough for finding exceptions and anomalies?
- - Yes for calibrated span-level thresholds (100% evidence at 15% of the spans, against 13-15% evidence for random sampling).- Application level vs. collector level adaptability?
- - Some advantages in application level: the app knows when a job ends, so `job_buffer` needs no hold time guess, and `span_filter_path` holds nothing and keeps the fewest spans. But `span_filter` reaches the same evidence at the collector without touching the app, `job_buffer` is the most sensitive to drift, and the in-app cost was not measured.

# 5. lesson

Problems faced

- Spent too long creating mock app
- - Mock app was too slow anyway
- - Replay made testing an agentic tracer more difficult

A proper compute cluster could have overcome the apps performance limitation, but the time budget did not allow to go deeper.

What we learned

- Decide on the span, not the trace: a slow call stands out on its own even when the whole job is long, while whole-trace latency barely separates anomalous from normal traces.
- Holding traces costs memory in proportion to rate times hold time, and a collector that runs out of memory loses everything it holds, not just the excess. Hold nothing where possible.
- Thresholds need calibration on a recent baseline, and drift matters: slower latencies than calibrated erase `job_buffer`'s reduction, faster ones start to cost evidence.
- Measure with a ground truth. Trace-level recall hides what span-level methods lose, which is why the evidence metric was added.
- Check the measurement before trusting it: two of the benchmark results were wrong at first (a replay bug that changed span IDs, and tail samplers compared at different hold times).

Adaptive tracing has a place but even in the age of AI is not a trivial thing.


# 6. Future work
- Can an LLM/agent keep up at reasonable cost? Dynamic sampling rate and reconfiguration (research question not answered, no agent was built or tested)
- Application-structure-aware reduction: weight the keep decision by how much a part of the application matters, instead of treating all spans alike.
- - Critical paths: keep every span on a job's critical path, and trim the parallel branches that do not set its duration.
- - Dependencies: keep the spans of services many other jobs depend on, and the first spans after a dependency fails or changes behaviour.
- - Business importance: give high-value jobs (for example the executive scorecard) a stricter anomaly threshold than low-value ones.
- Ideas that follow from the findings:
- - Learn thresholds online and detect drift automatically: re-calibrate when the share of spans flagged slow moves, since drift was the weakest point of `job_buffer` and of all calibrated methods.
- - Combine layers: an app-side `span_filter_path` to cut volume at the source and a collector-side tail sampler on the reduced stream, which makes the hold memory small.
- - Cover anomaly types that are not slow spans: error bursts, new span names, changed call counts or fan-out. A per-span latency threshold cannot see these.
- - Adaptive budget: raise or lower the thresholds to hit a target span rate or storage budget, instead of a fixed multiplier.
- - Keep context around an anomaly: when a span is flagged, also keep its neighbours and a few normal traces of the same job as comparison.
- - Measure the in-app cost (CPU, memory, added latency) of the reducers and replay at rates and anomaly frequencies closer to production.

# References

Tools and components the test bench is built on:

- OpenTelemetry. [Sampling concepts](https://opentelemetry.io/docs/concepts/sampling/), [Collector](https://opentelemetry.io/docs/collector/) and [Python SDK](https://opentelemetry.io/docs/languages/python/).
- OpenTelemetry Collector Contrib components: [tail sampling processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor), [probabilistic sampler processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/probabilisticsamplerprocessor), [filter processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/filterprocessor), [routing connector](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/connector/routingconnector), [file exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter) and [telemetrygen](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen).
- [Dagster](https://docs.dagster.io/), the orchestrator the mock app runs on.
- [Jaeger](https://www.jaegertracing.io/), for browsing traces.

Literature read for the course study logs is listed in [study_logs/README.md](study_logs/README.md).

# AI Usage

AI coding assistants (GitHub Copilot and Cursor, both using Claude models) were used extensively to implement this project's code, including the scenario model and validation, the executor and job factory, the API and clients, the scenario generator, the collector configs, the benchmark and evaluation scripts, the tests, this README and the presentation slides. The author directed the work (requirements, design decisions and review of the results), and the generated code was checked with the automated test suite and sample runs.
