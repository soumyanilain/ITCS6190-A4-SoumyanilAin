# Assignment #4: Ride-Sharing Analytics, from Stream to Model and Back

**ITCS 6190/8190 - Cloud Computing for Data Analysis - Fall 2026**

The ride-sharing platform of Assignment #3 and Hands-on L10 is back, with two sources of
data: a month of completed trips, and a live stream of rides. In Lecture 10 you learned to
process the stream with Structured Streaming; in Lecture 11, to train a model with MLlib
pipelines. This assignment puts the two together, the way it is done in practice: a
streaming job that aggregates and alerts, a batch job that trains a fare model and saves it,
and a scoring job that loads the model and applies it to the stream to flag rides whose fare
does not match the trip.

By the end you should be able to design and run the standard architecture for machine
learning on streaming data: **train in batch, save the pipeline, score the stream**.

**Worth 2 points** (the rubric is out of 100 and is scaled to 2). **Opens Tuesday, October
27. Due by 11:59 pm on Tuesday, November 10**, on Canvas. Work individually.

Two weeks, and part 2 depends on the November 3 lecture. A pace that works: **part 1 in the
first week** (everything it needs is in Lecture 10 and Hands-on L10), **parts 2 and 3 right
after November 3**, and the report in the last days. Part 3 is short once part 2 works.

---

## The data

`datagen.py` generates your data from a seed. Use your student ID: the data is then yours,
and the grader regenerates it from the seed you write in the report.

`shared-folder/input/trips_history.csv`, 60,000 completed trips in September 2026, the
training data:

| column         | example               | notes                                   |
| -------------- | --------------------- | --------------------------------------- |
| `trip_id`      | `T000001`             |                                         |
| `driver_id`    | `D081`                | 250 drivers                             |
| `pickup_zone`  | `5`                   | zone id, see zones.csv                  |
| `dropoff_zone` | `12`                  |                                         |
| `pickup_ts`    | `2026-09-20 08:06:04` |                                         |
| `distance_km`  | `4.85`                |                                         |
| `duration_min` | `16.2`                |                                         |
| `payment_type` | `card`                | card, app or cash                       |
| `fare`         | `17.23`               | the label; clean this time              |

`shared-folder/input/zones.csv`, 25 rows: `zone_id`, `zone_name`, `borough`.

`shared-folder/stream/rides_00.jsonl` to `rides_29.jsonl`: thirty minutes of live rides on
October 27, 2026, from 8:00 to 8:30, one file per minute of **arrival**, about 40 rides per
file, one JSON object per line:

```json
{"ride_id": "R00617", "driver_id": "D139", "pickup_zone": 5, "distance_km": 12.08, "duration_min": 33.5, "payment_type": "card", "fare": 51.65, "event_time": "2026-10-27 08:15:37"}
```

The fare depends on the distance, the duration, the zone, the hour and the payment type,
plus noise: a model can learn it. In the stream, a few percent of the rides carry a fare
that does not match the trip, far too high or far too low; those are the anomalies part 3
must find, and the generator does not mark them. Some events arrive late (event_time one
to two minutes before the file's minute), a few very late (eight to ten minutes), and a few
rides have a distance of zero. The files get modification times one minute apart, in
order, because Spark's file source reads new files oldest first: generate them where Spark
reads them and do not copy them around.

---

## The work

```
spark-submit main.py <part> <shared directory> <seed>
```

`main.py` is laid out like the hands-ons: the SparkSession, the two schemas, the streaming
source (`read_rides()`), the micro-batch summary, the feature derivation shared by parts 2
and 3 (`features_of`), the evaluators and the threshold rule of part 3 are given. Four
functions are yours; each docstring states exactly what to produce. The streaming parts
use one file per micro-batch and the `availableNow` trigger, as in Hands-on L10: the
thirty files are processed in order and the query stops, so a run is reproducible.

### Part 1: the stream (after Lecture 10)

**Part 1a, `part1a_zone_windows`.** Trips and revenue per pickup zone per 5-minute tumbling
window of `event_time`, with a 3-minute watermark, written in append mode to Parquet.
After the query, `main.py` prints the windows the stream wrote and the same aggregation
computed in batch over all the files. The report asks you to explain every difference
between the two: the windows that are missing and the trips that are missing.

**Part 1b, `part1b_alerts`.** A stateless alert stream: rides with a fare above 150 or a
distance of zero, written as they arrive to CSV with a `reason` column.

### Part 2: the fare model (after Lecture 11)

**`part2_train`.** On the historical trips, split with your seed: the **baseline**
(distance only, linear regression), the **full** pipeline (StringIndexer with
`handleInvalid="keep"` over zone, payment type and hour, OneHotEncoder, VectorAssembler
with the one-hot columns plus distance and duration, LinearRegression), and a **tuned**
model of your choice (a RandomForestRegressor or GBTRegressor on the index columns, or the
full pipeline with a different label or regularization) with a ParamGridBuilder grid of at
least two values of at least one parameter and a `CrossValidator(numFolds=3,
parallelism=2)`. Evaluate the three on the test set with RMSE and R², and save the
PipelineModel with the lowest RMSE under `shared-folder/models/fare` (for the tuned model,
`cvModel.bestModel`).

`features_of` derives the `hour` and `zone` columns from a DataFrame, as strings. It is
given and must not change: part 3 applies the same function to the stream, and a model
only works on the columns it was trained on.

### Part 3: scoring the stream (the integration)

**`part3_score`.** A scoring job: load the PipelineModel, apply `features_of` to the stream,
`transform`, compute `deviation = |fare - prediction|`, keep the rides whose deviation
exceeds the threshold (`main.py` computes it as 4 times the test RMSE of the saved model,
read from the part 2 metrics) and write them in append mode to CSV. No `fit()` anywhere in
this part. `main.py` then prints the flagged rides and their count per 5-minute window.

The generator can tell you which anomalies it planted, for the report only:
`python3 datagen.py <seed> shared-folder /tmp/truth.csv` writes them to the third argument.

### Rules

The DataFrame API, Structured Streaming and `pyspark.ml` only. No `pyspark.mllib`, no RDD
operations, no `collect()` followed by Python loops, no pandas, no scikit-learn.

---

## What is in this repository

| Path | What it is |
| ---- | ---------- |
| `main.py` | the application: setup is given, the four functions are yours |
| `datagen.py` | generates the trips, the zones and the stream files from a seed (plain Python) |
| `docker-compose.yml` | the Spark cluster of the hands-ons: one master, two workers, `apache/spark:4.2.0` |
| `docker-compose.codespaces.yml`, `conf/spark-defaults.conf` | the same cluster, for GitHub Codespaces |
| `REPORT.md` | the report template you fill in |
| `shared-folder/input/`, `shared-folder/stream/` | where `datagen.py` writes the data |
| `shared-folder/output/` | where the results land (`part1a/`, `part1b/`, `part2/`, `part3/`) |
| `shared-folder/checkpoint/`, `shared-folder/models/` | checkpoints and the saved model (not committed) |

---

## Steps

### 1. Make your own copy of this repository

On the repository page, click the green **Use this template** button, then **Create a new
repository**. Name it `ITCS6190-A4-<your-name>` and set the visibility to **Public**. Do not
fork and do not clone this repository directly. Clone *your* repository and work in it.

### 2. Generate your data

```bash
python3 datagen.py <your student ID>
```

Without Python on your machine, start the cluster first and run it in the master container:

```bash
docker cp datagen.py spark-master:/opt/spark/work-dir/shared/
docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py <your student ID> \
  /opt/spark/work-dir/shared
```

Open one of the last stream files and find a ride whose `event_time` is several minutes
before the others, and one whose fare looks wrong for its distance.

### 3. Start the cluster

```bash
docker compose up -d
```

Check <http://localhost:8080> for two workers in state ALIVE. In a **GitHub Codespace**, use
`docker compose -f docker-compose.codespaces.yml up -d` instead, as in the hands-ons, and add
the same `-f` flag to `docker compose down` at the end.

### 4. Run, fill in, run again

```bash
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared <your student ID>
```

As given, every part reports itself as not implemented. Complete one function at a time,
`docker cp` after **every** edit, and run again with `part1a`, `part1b`, `part2` or `part3`.

Three things to remember from the hands-ons:

- A streaming query remembers what it processed in its checkpoint. To run part 1a, 1b or 3
  again on the same files, delete `shared-folder/checkpoint/<part>` first (and the output
  folder of that part if you want it clean); otherwise the query finds nothing new.
- You cannot `show()` or `count()` a streaming DataFrame; `main.py` reads the outputs back
  in batch after each query, and you can do the same while developing.
- Part 2 runs several fits; with the grid of the reference it takes two to four minutes on
  the cluster. Open <http://localhost:4040> while it runs: each fit is a group of jobs, and
  the CrossValidator runs two of them at a time.

Useful commands:

```bash
ls shared-folder/output/part1a                               # one Parquet part per emitting batch
cat shared-folder/output/part1b/*.csv | grep -v ride_id | sort
cat shared-folder/output/part2/metrics/*.csv
cat shared-folder/output/part3/*.csv | grep -v ride_id | sort -t, -k9 -n -r | head
find shared-folder/models/fare -maxdepth 2
```

### 5. Stop the cluster

```bash
docker compose down
```

---

## What to commit

- Your completed `main.py`
- The results: `shared-folder/output/part1a/` (the Parquet part files), `part1b/`,
  `part2/metrics/` and `part3/` (the CSV part files; `_SUCCESS`, `_spark_metadata` and
  `.crc` files are ignored by `.gitignore`)
- `REPORT.md`, with your seed

The generated data, the checkpoints and the model are ignored on purpose: the grader
regenerates your data from the seed and reruns the parts. Leave `README.md`, `datagen.py`,
the compose files and the given parts of `main.py` as they are.

---

## Report

Fill in `REPORT.md`. Keep it short and specific; every answer should use your own numbers.

**Seed and commands.**

**Part 1.** The micro-batch summary of 1a and the explanation of every difference between
the stream's windows and the batch table: missing windows, missing trips, the state rows
pattern. The alerts of 1b and why that query needs no watermark.

**Part 2.** The three metrics lines; your tuned model, its grid, the number of fits and
whether it beat the linear pipeline; why `hour` is a category.

**Part 3.** The threshold, the flagged rides, the comparison with the planted anomalies
(precision and recall, in plain words), what the false positives have in common, why there
is no `fit()` and which stages of the loaded model are fitted models.

**Design.** One paragraph: this pipeline in production, with Kafka, nightly retraining and a
fraud team reading the alerts. Where each piece lives, how a new model reaches the running
scoring job, what you would monitor.

**Problems and fixes.** With the actual error messages. If nothing went wrong, say so.

---

## Grading (100 points, scaled to 2)

| Criterion | Points |
| --------- | ------ |
| Repository created from the template, public, with the required files and nothing else | 10 |
| Part 1: windows with watermark in append mode and the alert stream, correct for your seed; the summary and the differences explained | 25 |
| Part 2: baseline and full pipelines correct for your seed; a tuned model with a real grid and CrossValidator; the best PipelineModel saved | 25 |
| Part 3: a pure scoring job that loads the pipeline, flags by deviation and catches the planted anomalies; precision and recall discussed | 25 |
| Report: specific to your data, the design paragraph shows the train/save/score architecture, problems quoted with fixes | 15 |

---

## Submission

Post the URL of **your** repository on Canvas by the deadline. Keep it public until grades
are posted. There is no need to add the instructor or the TAs as collaborators.

---

## Optional: go further

Not graded. Replace the console of part 1a with `foreachBatch` writing the windows to a
table you can query while the stream runs; or add a `dropDuplicates` on `ride_id` with a
watermark to part 3 and feed it a stream folder where a file was copied twice; or run part 3
with the default trigger instead of `availableNow` and copy new files into the stream
folder while it runs.
