// A5 — Batch vs Streaming (Track A, story-sim T3)
// Uses trace engine/traces/a5-batch-vs-streaming.json.
// Verified arithmetic used in checks: batch $18/day vs streaming $310/day ≈ 17x;
// latency batch 86,400s (~24h) / micro 120s / streaming 3s.
export default {
  id: "A5",
  track: "A",
  title: "Batch vs Streaming",
  minutes: 20,
  coldOpen: "NimbusMart's first fraud pipeline ran nightly: every order from the day was scored and queued for review at 02:00. Then a fraudster placed a large order at 08:15, it auto-fulfilled and shipped by noon, and the review queue only flagged it nineteen hours later — long after the goods were gone. Stung, the team rebuilt the whole thing as real-time streaming, cut latency to three seconds, and watched their compute bill jump seventeen-fold along with a new pager rotation for stalled streams. Both designs were defensible. Neither was right. The right answer was a third thing sitting quietly between them.",
  concept: [
    { type: "prose", html: `
<p>Every processing architecture is a point on one triangle, and the triangle's rule is unforgiving: <strong>latency, throughput, and cost — you get to optimize two, and the third moves against you.</strong> The same NimbusMart order-events workload runs three ways, and the difference is entirely which two corners you pin:</p>
<ul>
<li><strong>Nightly batch.</strong> One job processes a bounded chunk — the whole day's events — on a schedule. Throughput is enormous (the cluster is fully saturated for one pass) and cost is tiny (it lives ~25 minutes a day, then shuts off). The corner you give up is latency: an event at 02:01 waits nearly 24 hours for the next run.</li>
<li><strong>Real-time streaming.</strong> A continuously running job processes events as they arrive — latency in seconds. But the cluster is always on and provisioned for the <em>peak</em>, so throughput-per-dollar drops and cost climbs. You give up cheapness (and simplicity) to buy freshness.</li>
<li><strong>Micro-batch.</strong> The pragmatic middle: the streaming API, but triggered every couple of minutes, processing each small window as a mini-batch. Latency of about two minutes, throughput back near batch levels, cost moderate. It deliberately surrenders sub-second latency — which most workloads don't need — to keep the other two corners reasonable.</li>
</ul>
<p>The cold open is what happens when you treat this as a binary. Batch was too slow for fraud; the team leapt to streaming and overpaid massively for latency far finer than a human-reviewed queue can even use. Micro-batch was the point on the triangle that fit the actual requirement.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="11">
<text x="360" y="20" text-anchor="middle" fill="var(--ink2)" font-size="11">THE LATENCY / THROUGHPUT / COST TRIANGLE — pin two, the third moves against you</text>
<polygon points="360,44 116,214 604,214" fill="none" stroke="var(--line)" stroke-width="1.5"/>
<text x="360" y="38" text-anchor="middle" fill="var(--ink2)">LOW LATENCY</text>
<text x="96" y="230" text-anchor="middle" fill="var(--ink2)">HIGH THROUGHPUT</text>
<text x="624" y="230" text-anchor="middle" fill="var(--ink2)">LOW COST</text>
<!-- streaming: near low-latency corner -->
<circle cx="360" cy="86" r="7" fill="var(--rust)"/>
<text x="360" y="76" text-anchor="middle" fill="var(--rust)" font-size="10">STREAMING</text>
<text x="360" y="108" text-anchor="middle" fill="var(--ink2)" font-size="9">~3 s · $310/day · always-on</text>
<!-- batch: near throughput+cost edge -->
<circle cx="180" cy="196" r="7" fill="var(--accent)"/>
<text x="180" y="186" text-anchor="middle" fill="var(--accent)" font-size="10">NIGHTLY BATCH</text>
<text x="180" y="252" text-anchor="middle" fill="var(--ink2)" font-size="9">~24 h · $18/day · cheap</text>
<!-- micro-batch: centroid -->
<circle cx="372" cy="165" r="8" fill="var(--green)"/>
<text x="440" y="160" fill="var(--green)" font-size="10">MICRO-BATCH</text>
<text x="440" y="176" fill="var(--ink2)" font-size="9">~2 min · $74/day</text>
<text x="440" y="190" fill="var(--ink2)" font-size="9">the 90% default</text>
<text x="360" y="248" text-anchor="middle" fill="var(--ink2)" font-size="9">match the corner you pin to what the consumer actually needs — human queue, dashboard, or a system that acts in milliseconds</text>
</svg>`, caption: "Batch pins throughput + cost (loses latency); streaming pins latency + throughput (loses cost); micro-batch sits deliberately in the middle of all three — good enough on every axis, optimal on none." },
    { type: "prose", html: `
<p>Why micro-batch is the honest default deserves unpacking, because it's a Structured Streaming trick, not a separate system. You write a <em>streaming</em> query, then set a <strong>trigger</strong>: <code>trigger(processingTime='2 minutes')</code>. The engine wakes every two minutes, reads whatever arrived since the last checkpoint, processes it as one small batch with normal wide, efficient tasks, commits, and sleeps. You get streaming's continuous, fault-tolerant, exactly-once machinery (checkpoints, offset tracking) with batch's throughput profile — because fat two-minute batches schedule far better than a torrent of one-record tasks.</p>
<p>The decision rule writes itself once you stop asking "how fresh <em>can</em> it be?" and start asking "how fresh does the <em>consumer</em> need?":</p>
<ul>
<li><strong>Nightly batch</strong> for finance roll-ups, the daily executive dashboard, backfills — anything read on a human-day cadence. Latency of hours is invisible.</li>
<li><strong>Micro-batch</strong> for the fraud-review queue and most operational marts. A queue a human works can't tell 2 minutes from 3 seconds — and it costs a fraction of streaming. This is the 90% case.</li>
<li><strong>True streaming</strong> only where <em>seconds change a decision</em>: payment authorization, live inventory holds, security alerting. Here the latency isn't vanity — a slower answer is a wrong answer.</li>
</ul>
<p>One streaming-specific wrinkle you'll meet properly later: <strong>late data</strong>. NimbusMart's <code>courier_pings</code> arrive with <code>ingested_at</code> lagging <code>event_ts</code>, sometimes by over an hour. Streaming and micro-batch handle this with <strong>watermarks</strong> — "wait up to N minutes for stragglers, then close the window." Pure nightly batch sidesteps it by simply processing a whole settled day at once. More freshness means more machinery to handle time correctly; that machinery is part of streaming's true cost.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

# The SAME fraud-scoring logic, three clocks. Only the read/write wrapper changes.

# (a) NIGHTLY BATCH — bounded read of yesterday's partition, cheap, high-latency
batch = (spark.read.parquet("s3://nimbus/lake/order_events")
    .filter(F.col("ds") == "2026-07-08"))
batch.write.mode("overwrite").saveAsTable("gold.review_queue")   # runs once at 02:00

# (b) REAL-TIME STREAMING — continuous, seconds of latency, always-on cluster
stream = (spark.readStream.format("delta").table("bronze.order_events")
    .withWatermark("event_ts", "10 minutes"))                    # handle late pings
(stream.writeStream.format("delta")
    .option("checkpointLocation", "s3://nimbus/ckpt/review_rt")
    .trigger(continuous="1 second")                              # priciest freshness
    .toTable("gold.review_queue"))

# (c) MICRO-BATCH — same streaming API, batched every 2 min: the pragmatic middle
(stream.writeStream.format("delta")
    .option("checkpointLocation", "s3://nimbus/ckpt/review_mb")
    .trigger(processingTime="2 minutes")                         # batch-like throughput,
    .toTable("gold.review_queue"))                               # 2-min latency, moderate cost`, caption: "One logic, three triggers. Micro-batch (c) is streaming's fault-tolerance with batch's throughput — the trigger is the entire difference in cost and latency." },
    { type: "analogy", title: "The nightly truck, the conveyor belt, and the two-minute shuttle", html: `
<p>NimbusMart has to move parcels from the receiving dock to the sorting hall. Three ways to run it.</p>
<p>The <strong>nightly truck</strong> waits until 2 a.m., loads the entire day's parcels in one go, and makes a single trip. Maximally efficient per parcel — the truck runs 25 minutes and sits idle the other 23.5 hours — and dirt cheap. But a parcel that lands at 8 a.m. sits on the dock for eighteen hours waiting for the truck. That's batch.</p>
<p>The <strong>always-running conveyor belt</strong> carries each parcel to sorting the instant it's set down — seconds of delay. Wonderful responsiveness, but the belt runs 24/7 whether one parcel arrives or a thousand, drawing power and needing an operator on shift around the clock, sized for the busiest minute of the year. That's streaming: you pay for the peak, continuously.</p>
<p>The <strong>two-minute shuttle</strong> is the compromise a real warehouse actually runs: a cart that departs the dock every two minutes with whatever has accumulated. Nobody waits more than two minutes, each trip is a full efficient load, and the cart parks between runs instead of burning power all shift. That's micro-batch — and for a sorting hall staffed by humans, no one can tell the two-minute shuttle from the instant belt, so why pay for the belt?</p>` },
    { type: "javaBridge", html: `
<p>You've shipped all three of these; they had Spring annotations on them.</p>
<ul>
<li><strong>Batch ≈ your <code>@Scheduled</code> cron ETL.</strong> A job that wakes on a schedule, processes a bounded set (last night's rows), writes, and exits. Simple, cheap, easy to reason about — and exactly as stale between runs as its cadence.</li>
<li><strong>True streaming ≈ a <code>@KafkaListener</code> / message-driven consumer.</strong> An always-on process reacting to each event as it lands. You already know its costs: it runs 24/7, you provision for peak load, and you inherit the operational weight — offset management, redelivery, back-pressure, on-call for when the consumer stalls.</li>
<li><strong>Micro-batch ≈ a poll-and-batch consumer.</strong> Think <code>consumer.poll(Duration.ofMinutes(2))</code> draining a batch of records each cycle rather than reacting per-message. Fewer, fatter units of work — better throughput, bounded latency, less overhead — which is precisely why it's the sensible default.</li>
<li><strong>Exactly-once ≈ idempotent offset commits.</strong> Structured Streaming's checkpoint + write-ahead machinery is the same discipline as committing Kafka offsets only after a successful, idempotent write — so a restart replays without double-counting. It's not magic; it's the transactional-consumer pattern you already trust, run at table scale.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "a5-batch-vs-streaming",
      task: `<p><strong>Run one workload — NimbusMart order-events feeding the fraud-review queue — three ways, and read the triangle off the bars.</strong> Scrub the timeline and compare across the three architectures:</p>
<ul>
<li><em>Latency</em> (t=3, t=9, t=15, t=18): ~24 hours (batch) → ~2 minutes (micro-batch) → ~3 seconds (streaming). Five orders of magnitude across the same data.</li>
<li><em>Cost</em> (t=6, t=12, t=21): $18/day (batch) → $74/day (micro-batch) → $310/day (streaming). The cost axis points the opposite way from latency — that's the triangle.</li>
<li><em>The decision</em> (t=24): match latency to the consumer. For a queue a <em>human</em> works, 2-minute micro-batch is indistinguishable from real-time at a fraction of the cost — which is why it's the default, and why the cold open's leap to full streaming was overkill.</li>
</ul>
<p>The point to internalize: there's no free architecture. Cheaper freshness always bills somewhere — usually in cost, sometimes in operational weight.</p>`
    },
    buildWithAI: `I'm learning batch vs streaming vs micro-batch for data engineering. Build me a real local PySpark project that runs the SAME fraud-scoring logic all three ways so I can feel the latency/throughput/cost trade-off. Assume nothing beyond Python 3.10+; I'm on Windows (use pathlib, no bash-isms). Spark Structured Streaming works fine locally against a folder source.

1. Create a folder \`nimbusmart-batch-stream\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_events.py\` — deterministic (random.seed(42)) — that can DRIP order events into \`data/incoming/\` as newline-JSON files, one small file every ~1 second when run with --stream, or all at once when run with --batch. Each event: event_id (unique), order_id (O-100001..), fraud_score (0.00..0.99, 2dp), event_ts (ISO). Also write a \`fraud_scores\` lookup so the score is joinable. Print how many events it emitted.

3. Create \`pipeline.py\` with ONE shared transform function build_queue(df) that defines FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant and filters events to fraud_score >= FRAUD_REVIEW_THRESHOLD (the review queue). Then expose three runners that all call build_queue:
   - run_batch(): spark.read the whole data/incoming folder once, write the queue to out/batch, print wall-clock.
   - run_micro_batch(): spark.readStream the folder with trigger(processingTime='10 seconds'), writeStream to out/micro with a checkpoint; run for ~60s; print how many micro-batches fired and the per-batch latency.
   - run_streaming(): spark.readStream the folder with trigger(processingTime='0 seconds') (as-fast-as-possible, the closest local proxy for continuous); writeStream to out/stream with a checkpoint; print batch cadence.
   Add a tiny --mode {batch,micro,stream} CLI so I can run each.

4. Create \`test_pipeline.py\` (pytest) asserting:
   - all three modes, fed the SAME complete input, produce the SAME set of order_ids in the review queue (the architecture must not change the result — recompute expected from the raw JSON with the plain json module, don't hardcode).
   - build_queue keeps exactly the events with fraud_score >= FRAUD_REVIEW_THRESHOLD and drops the rest.
   - the micro-batch run produced MORE THAN ONE committed batch (proving it's incremental), while run_batch produced exactly one output commit.

5. Run: generate --batch, then pytest; then in one terminal generate --stream (dripping files) and in another run pipeline --mode micro to watch batches fire every 10s. Explain in comments how trigger interval is the single knob that moves you along the latency/cost trade-off, and why the OUTPUT is identical across all three while the COST and FRESHNESS are not.`
  },
  check: [
    {
      type: "mcq",
      q: "Micro-batch is described as the pragmatic middle of the latency/throughput/cost triangle. What does it deliberately trade away, and to gain what?",
      options: [
        "It gives up sub-second latency (accepting ~minutes) to keep batch-like throughput and moderate cost — the corner most workloads don't actually need",
        "Nothing — micro-batch optimizes latency, throughput, and cost all at once, which is why it's always the best choice",
        "It gives up throughput to achieve the lowest possible cost, making it slower than nightly batch",
        "It gives up correctness — micro-batch can double-count events that true streaming would not"
      ],
      answer: 0,
      explain: "The triangle lets you pin two corners; micro-batch surrenders sub-second freshness (which a human-reviewed queue can't use anyway) to hold onto throughput and reasonable cost. 'It optimizes all three' is the misconception the triangle exists to kill. And it's exactly-once via checkpoints, so correctness is not the thing sacrificed."
    },
    {
      type: "predict",
      q: "The same order-events workload costs $18/day as nightly batch and $310/day as always-on real-time streaming. Roughly what cost multiple is streaming?",
      code: `batch_cost = 18     # nightly batch, cluster up ~0.4 h/day
stream_cost = 310   # streaming, cluster up 24 h/day, sized for peak
print(round(stream_cost / batch_cost))   # x`,
      options: ["2", "17", "60", "1"],
      answer: 1,
      explain: "310 ÷ 18 ≈ 17×. That's the price of turning ~24 hours of latency into ~3 seconds on this workload — a 24/7 cluster provisioned for peak load. Whether 17× is worth it depends entirely on whether the consumer needs seconds; for a human-worked review queue, it isn't."
    },
    {
      type: "mcq",
      q: "For which of these NimbusMart workloads is <em>true</em> real-time streaming actually justified, rather than micro-batch?",
      options: [
        "The daily executive revenue dashboard, refreshed each morning",
        "Blocking a payment at checkout when the live fraud model rejects it — a decision that must complete in milliseconds while the customer waits",
        "The fraud-review queue that human analysts work through during business hours",
        "Every workload — fresher data is always better, so streaming should be the default"
      ],
      answer: 1,
      explain: "Streaming earns its cost only when seconds change a decision — a synchronous payment block is exactly that. The daily dashboard is fine on batch; the human-worked review queue can't tell 2 minutes from 3 seconds, so micro-batch wins. 'Fresher is always better' (the misconception) ignores that freshness you can't use is pure cost."
    },
    {
      type: "mcq",
      q: "How does micro-batch achieve batch-like throughput while still using the streaming API?",
      options: [
        "It processes one record at a time like true streaming, but caches results between records",
        "A trigger (e.g. <code>processingTime='2 minutes'</code>) makes the streaming engine accumulate events and process each window as one efficient mini-batch — fat, well-scheduled tasks instead of a torrent of tiny per-record ones",
        "It converts the stream to a nightly batch job under the hood and runs it once",
        "It skips checkpointing to run faster, giving up exactly-once guarantees"
      ],
      answer: 1,
      explain: "The trigger interval is the whole trick: the engine buffers events and runs each interval as a small batch, so tasks are fat and schedule efficiently (batch's throughput profile) while checkpoints preserve exactly-once. Processing one record at a time (the misconception) is what makes plain per-record streaming expensive — micro-batch exists precisely to avoid it."
    }
  ],
  fieldNotes: `A payments-adjacent marketplace ran fraud review as a nightly batch to save money — score everything at 02:00, queue it for the analysts. It worked until a fraud ring learned the cadence and timed large orders for just after 03:00, giving them a ~23-hour head start; several shipped and were lost before review. The team's over-correction was dramatic: a full real-time streaming rebuild, continuous trigger, always-on cluster. Latency dropped to ~4 seconds, the monthly compute bill rose roughly 18×, and they added a weekend on-call rotation for stalled streams and watermark tuning on late courier pings. Six weeks later a senior engineer asked the question nobody had: 'how fast do the human analysts actually work the queue?' The answer was minutes, in business hours. They moved to a 90-second micro-batch trigger, kept the fraud window closed tight enough to stop the ring, and cut the streaming bill by more than 80%. The takeaway they wrote down: the requirement was never 'real-time', it was 'faster than a fraudster and no slower than a human needs' — and that number is micro-batch, not streaming.`
};
