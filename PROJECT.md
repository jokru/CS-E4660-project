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
