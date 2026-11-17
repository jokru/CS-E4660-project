# Project idea

## The problem

- In a production application, a lot of requests
  - Trace storage becomes a cost factor
- What to trace becomes an important question
- Existing methods:
  - Random sampling (store 1% of traces)
  - Anomalies (store only if duration > 1s)
- Without application specific context, observability is limited

## The project

- Build a small mock microservice application
  - Multiple components: DB, cloud storage, processing, LLM
  - Injectable faults and delays
- Try multiple different versions of a trace reducer
  - Naive
  - Sampling
  - Anomalies + sampling
  - Statistical sampling (keep traces in proportion to how unusual they are versus a history of error rates and latencies)
  - LLM agent
  - Increasing trace resolution when an anomaly happens
  - Logic in collector vs. application
  - Combinations
- Increase traces/s and see how each holds up

## The output

- Seeing how each method of trace reduction responds to increasing number of traces (i.e. application complexity)
- Do simple reducers miss rare exceptions?
- Does a smarter reducer matter enough for finding exceptions and anomalies?
- Can an LLM keep up with reasonable costs?
- Application level vs. collector level adaptability?

## Notes/ideas

- per component tracing parameters
- agent for configuration
- time based differences in sampling
- different faults (latency, full errors)
- different faults per component?
- statistical sampling: per-key (service/operation) history of error rate and latency percentiles
  - history store: in-memory counters on the hot path, periodically persisted or refreshed from a DB (SQLite/Redis); a per-trace DB lookup may not keep up at high rates
  - cold start: keep everything for the first N traces, seed from a prior seeded run, or decay toward a prior
  - drift: sliding window or EWMA, so a sustained outage does not become the new normal (test with the `email_outage` / `http_flap` windows)
  - needs custom code (no stock collector policy): a small service beside the collector or a custom processor
  - evaluation: train the history on a separate seeded run; compare a learning-during-run variant with a pre-trained one
- make anomaly rates tunable (about 1% or less) so "do simple reducers miss rare exceptions?" is meaningful
