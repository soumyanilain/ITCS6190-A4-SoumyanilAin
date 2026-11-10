# Assignment #4: Report

**Name:** Soumyanil Ain
**Student ID:** 801488534
**Email:** sain@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: **801488534**

```
seed 801488534: 60000 historical trips, 30 stream files with 1168 rides (49 fares that do not
match the trip, 67 late by 1-2 minutes, 14 late by 8-10 minutes, 14 with zero distance)
```

The commands you ran, in order:

On Windows, in PowerShell, with Docker Desktop:

```powershell
python datagen.py 801488534
# planted anomalies, written to a separate folder so the stream files Spark reads are not rewritten
python datagen.py 801488534 "$env:TEMP\a4truth" "$env:TEMP\truth.csv"

docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/

# numpy is not in the apache/spark image (see Problems and fixes)
foreach ($c in "spark-master","spark-worker-1","spark-worker-2") {
  docker exec -u root $c bash -c "python3 -m pip install -q numpy || python3 -m pip install -q --break-system-packages numpy || (apt-get update -qq && apt-get install -y -qq python3-numpy)"
}

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared 801488534
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part1b /opt/spark/work-dir/shared 801488534
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part2 /opt/spark/work-dir/shared 801488534
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part3 /opt/spark/work-dir/shared 801488534

docker compose down
```

Each part ran once on a clean `shared-folder`. The first part 1a run failed on import before
Spark started (numpy), so it left no checkpoint behind. To rerun a streaming part, delete
`shared-folder/checkpoint/<part>` and `shared-folder/output/<part>` first.

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows):

```
batch input rows            watermark state rows
    0         35                    -         21
    1         35  2026-10-27 07:57:59         29
    2         43  2026-10-27 07:58:57         31
    3         40  2026-10-27 07:59:53         33
    4         37  2026-10-27 08:00:57         25
    5         40  2026-10-27 08:01:55         38
    6         39  2026-10-27 08:02:52         47
    7         35  2026-10-27 08:03:49         48
    8         37  2026-10-27 08:04:50         48
    9         42  2026-10-27 08:05:46         24
   10         44  2026-10-27 08:06:52         43
   11         37  2026-10-27 08:07:53         46
   12         37  2026-10-27 08:08:52         48
   13         40  2026-10-27 08:09:45         48
   14         35  2026-10-27 08:10:47         25
   15         42  2026-10-27 08:11:47         43
   16         39  2026-10-27 08:12:58         47
   17         35  2026-10-27 08:13:44         49
   18         45  2026-10-27 08:14:47         50
   19         40  2026-10-27 08:15:55         25
   20         38  2026-10-27 08:16:54         40
   21         43  2026-10-27 08:17:56         45
   22         40  2026-10-27 08:18:47         48
   23         41  2026-10-27 08:19:55         48
   24         41  2026-10-27 08:20:56         25
   25         37  2026-10-27 08:21:55         42
   26         35  2026-10-27 08:22:51         46
   27         36  2026-10-27 08:23:52         48
   28         42  2026-10-27 08:24:50         50
   29         38  2026-10-27 08:25:51         25
   30          0  2026-10-27 08:26:57         25
```

Which 5-minute windows did the stream write, and which are missing compared with the batch
table printed after the query? For one window that was written, compare its trips with the
batch value: where did the difference come from (look at how `datagen.py` makes the late
events)? Why does `state rows` drop every five batches?

The stream wrote six windows, 07:55 through 08:20 (133 zone rows, 981 trips). The batch table
has seven windows (158 rows, 1168 trips). The one missing is **08:25-08:30**: all 25 zones,
175 trips, 3931.08 in revenue. In append mode a window is written only once it is final, and
that happens when the watermark passes its end. The last event is at about 08:29:57, so the
watermark stops at 08:26:57 (batch 30, the no-data batch Spark runs after the last file).
08:26:57 is before 08:30, so that window is still open in the state when the query stops,
and nothing is ever written for it. The 07:55 window exists because rides in the first file
can have an event_time up to 30 seconds before their arrival minute: 10 trips in 9 zones.

The other 12 missing trips are inside windows that were written:

| window | batch trips | stream trips | dropped                                        |
| ------ | ----------: | -----------: | ---------------------------------------------- |
| 08:00  |         191 |          189 | R00501, R00539                                 |
| 08:05  |         197 |          195 | R00588, R00641                                 |
| 08:10  |         197 |          191 | R00798, R00810, R00818, R00861, R00872, R00874 |
| 08:15  |         201 |          199 | R01019, R01115                                 |
| 08:20  |         197 |          197 | none                                           |

Take the 08:10-08:15 window: 197 trips in batch, 191 in the stream. The six missing rides
are all "very late" events, made by the `k >= 12 and r < 0.09` branch of `datagen.py`, which
sets `event = arrival - 480..600 s`. For example, R00861 sits in `rides_22` (arrival 08:22)
with event_time 08:12:12, in zone 5, so zone 5 has 24 trips in batch and 23 in the stream.
The window closed at batch 19, when the watermark reached 08:15:55. Files 20, 21 and 22 came
after that, so Spark dropped their rides for this window as too late. The 67 rides that are
late by 1-2 minutes all made it, since the 3-minute watermark covers them.

There is one thing I did not expect. The generator made 14 very late rides, but only 12 were
dropped. The two survivors, R00739 and R00742 (event_time 08:10:10 and 08:10:31, in
`rides_19`), arrived in the same micro-batch that closed their window. Spark filters late
input rows with the watermark of the previous batch (08:14:47, which is before the window
end, so they are kept) and evicts windows with the current one (08:15:55). So they were
added to the state and written with the window. I checked this by recomputing both rules
from the files: "drop if window end <= previous batch's watermark" gives exactly 12.

State rows: there is one row per (window, zone) that is still open. Each file adds rows as
new zones show up in the current window. Files are one minute apart and windows are five
minutes long, so every fifth batch (4, 9, 14, 19, 24, 29) the watermark crosses a window end
(08:00, 08:05, ...). That window is written and its rows are removed from the state, which
leaves only the current window's zones (24 or 25 rows). It is batch 4 and not batch 5
because the watermark of batch 4 already comes from file 3's latest event, 08:03:57, minus
3 minutes = 08:00:57.

## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

```
14 alerts: zero_distance 14, high_fare 0

ride_id,event_time,pickup_zone,distance_km,duration_min,fare,reason
R00034,2026-10-27 08:00:08,9,0.0,9.2,11.02,zero_distance
R00255,2026-10-27 08:05:49,1,0.0,35.2,38.1,zero_distance
R00263,2026-10-27 08:06:29,4,0.0,14.2,13.59,zero_distance
R00382,2026-10-27 08:09:09,2,0.0,13.6,18.73,zero_distance
R00520,2026-10-27 08:13:14,13,0.0,11.0,12.02,zero_distance
```

All 14 zero-distance rides from the generator were caught. No ride in my stream has a fare
above 150 (the highest is R01136 at 126.80), so the fixed rule finds none of the 29 planted
"too high" fares. R01136 is a planted anomaly at about three times its real price. Part 3
catches it because it compares the fare with what the trip should cost.

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?

Each alert depends only on the row itself: a filter and a `when()`. Nothing has to be
remembered between micro-batches, so there is no state to clean up, and that is the only
thing a watermark is for. A late event is still a valid alert, and the query just writes it
when it arrives. Nothing is ever dropped for being late here, unlike part 1a.

"More than three alerts for the same driver in ten minutes" needs memory across batches. I
would keep `driver_id` in the alert stream, then
`withWatermark("event_time", ...)`, `groupBy("driver_id", window("event_time", "10 minutes"))`,
`count()` and `filter(count > 3)`. Now there is state per driver and window, so a watermark is
required. Without it the state grows forever, and in append mode nothing would ever be
written. I would also have to pick how late an alert may be and still count. In append mode
the alert only comes out when the window closes, up to 10 minutes plus the watermark after
the first event. Update mode would emit it as soon as the count passes 3. A tumbling window
also misses four alerts that straddle a window boundary. A sliding window
(`window(..., "10 minutes", "1 minute")`) or `transformWithState` with a per-driver timer
fixes that.

---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```
seed 801488534: 48002 training rows, 11998 test rows
baseline   RMSE   3.71   R2  0.859
full       RMSE   1.71   R2  0.970
cross-validation: 4 combinations x 3 folds + 1 refit = 13 fits, 43 s
   maxDepth=8, maxBins=32: mean CV RMSE 2.248
   maxDepth=8, maxBins=128: mean CV RMSE 2.173
   maxDepth=10, maxBins=32: mean CV RMSE 2.260
   maxDepth=10, maxBins=128: mean CV RMSE 2.212
tuned      RMSE   2.07   R2  0.956
tuned: RandomForestRegressor numTrees=30, best maxDepth=8, maxBins=128
best model: full (test RMSE 1.71), saved under /opt/spark/work-dir/shared/models/fare
part 2 took 58 s
```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?

I used a `RandomForestRegressor` (30 trees, seed 801488534) on the StringIndexer output plus
distance and duration. There is no one-hot: the index columns carry nominal metadata, so the
trees split on zone, payment type and hour as categories. The grid has `maxDepth` in {8, 10}
and `maxBins` in {32, 128}. I picked depth because the surge is an interaction (hour times
distance) and a tree needs some depth to express it. I picked bins because the fare is a
straight line in distance and duration, and a tree can only draw that line as steps; 32
candidate split points is coarse for a distance that runs from 0.3 to over 30 km. More bins
helped (2.248 to 2.173 at depth 8), and more depth slightly hurt. That is 4 combinations x 3
folds = 12 fits, plus 1 refit of the best combination on the whole training set: 13 fits.
Part 2 took 58 s end to end on the cluster, 43 s of it in the CrossValidator.

The tuning did not beat the full linear pipeline: 2.07 against 1.71. That says the data is
close to linear. In `datagen.py` the fare is a base plus 1.6 per km plus 0.35 per minute,
fixed bumps for three zones and for cash, and Gaussian noise with a standard deviation of
1.5. Linear regression gets that shape exactly with two slopes and some dummies, while 30
trees can only approximate it with steps. The full model at 1.71 is already close to the 1.5
noise floor. The gap left is the surge: it multiplies distance and duration by 1.25 at 7, 8,
17 and 18 h, and a one-hot hour can only add a flat amount (see part 3).

Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?

As a number, linear regression would fit one slope for hour, as if fares rose steadily from
midnight to 23:00. The real effect is a bump at rush hours (7, 8, 17, 18) and nothing in
between, and 23:00 and 00:00 are neighbours in time but 23 apart as numbers. As a category,
each hour gets its own coefficient. The saved model learned hour 8 at +4.24 over 3 am, and
hours 7, 17 and 18 at almost the same value, which is exactly the surge pattern.

---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```
saved model: full, test RMSE 1.71; flagging rides with deviation > 6.84

=== part3: 71 rides flagged ===
+-------+-------------------+-----------+-----------+------------+------------+-----+----------+---------+
|ride_id|event_time         |pickup_zone|distance_km|duration_min|payment_type|fare |prediction|deviation|
+-------+-------------------+-----------+-----------+------------+------------+-----+----------+---------+
|R01136 |2026-10-27 08:29:42|1          |10.5       |34.6        |app         |126.8|38.78     |88.02    |
|R00152 |2026-10-27 08:03:17|23         |8.1        |26.2        |card        |99.29|29.19     |70.1     |
|R00474 |2026-10-27 08:12:40|16         |7.29       |30.6        |cash        |95.65|30.47     |65.18    |
|R00588 |2026-10-27 08:06:43|23         |8.49       |35.6        |card        |94.5 |33.3      |61.2     |
|R00102 |2026-10-27 08:00:49|2          |8.47       |34.4        |card        |92.77|32.83     |59.94    |
|R00498 |2026-10-27 08:12:06|11         |8.63       |34.8        |app         |87.41|33.23     |54.18    |
|R00380 |2026-10-27 08:08:54|11         |7.34       |22.9        |app         |77.31|26.71     |50.6     |
|R00112 |2026-10-27 08:01:10|4          |8.03       |27.9        |card        |79.48|29.72     |49.76    |
|R00186 |2026-10-27 08:04:26|23         |4.36       |20.5        |card        |61.79|20.89     |40.9     |
|R00128 |2026-10-27 08:03:30|12         |5.69       |15.3        |app         |62.04|21.16     |40.88    |
+-------+-------------------+-----------+-----------+------------+------------+-----+----------+---------+

=== part3: flagged rides per 5-minute window (batch over the output) ===
+-------------------+-----+
|       window_start|count|
+-------------------+-----+
|2026-10-27 08:00:00|   12|
|2026-10-27 08:05:00|   12|
|2026-10-27 08:10:00|   15|
|2026-10-27 08:15:00|   11|
|2026-10-27 08:20:00|    8|
|2026-10-27 08:25:00|   13|
+-------------------+-----+
```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?

The generator planted 49 anomalies: 29 fares too high and 20 too low. **All 49 are in my 71
flagged rides**, so recall is 100%: nothing planted got through. The closest call was a low
one, R00562 (fare 3.61, prediction 12.72, deviation 9.11), still well above 6.84. **22 of the
71 were not planted**, so precision is 49/71 = 69%: about two out of three alerts are real
problems. Also, R00588 and R00874 are among the very late rides that part 1a dropped, and
both are planted anomalies. Part 3 has no state and no watermark, so a late ride is still
scored.

The 22 false positives come in two groups, and in all of them the fare is higher than the
prediction:

- **6 zero-distance rides** (R00553, R00255, R01070, R00983, R01068, R00382). The generator
  priced the real trip and then set the distance to 0, so the model sees a 33-minute ride
  that went nowhere and predicts too little. These are data errors that part 1b already
  reports, not fare anomalies.
- **16 long trips**, 12 to 27 km and 35 to 81 minutes, with deviations of 7 to 11. All the
  stream rides are at 8 am, a surge hour. The model learned the surge as a flat +4.24 for
  hour 8, which is about right for an average 5 km trip. But the real surge is 1.25 times
  the distance and duration part, so on a 20 km, 60-minute trip it is worth about 14, and
  the model comes up roughly 7 to 10 short.

To reduce them, I would let the model learn a separate per-km and per-minute price for each
hour: add an `Interaction` stage between `hour_vec` and a vector of (distance, duration) to
the full pipeline. It stays inside the pipeline, so part 3 and `features_of` do not change.
I tried it as a side test: test RMSE drops to 1.50 (the noise floor), the threshold becomes
6.00, and the stream gives 58 flags, still 49/49 caught, with all 16 long-trip false
positives gone. The 9 that are left are all zero-distance rides. Skipping
`distance_km == 0` in part 3, since 1b already reports those, would take precision to 100% on
this data.

Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?

Learning happens once, in batch, on a month of history, and the result is saved. The scoring
job only applies it. A `fit()` needs to read all of its input, often several times (count the
labels, solve the regression), and a stream has no end. Spark refuses it before it starts. I
tried `StringIndexer.fit()` and `LinearRegression.fit()` on the streaming DataFrame and both
fail with:

```
AnalysisException: Queries with streaming sources must be executed with writeStream.start(),
or from a streaming table or flow definition within a Spark Declarative Pipeline.;
```

Even if it worked, fitting on the stream would learn from the anomalies we are trying to
catch, and the model would change every minute.

The loaded model has four stages:

- `StringIndexerModel` (fitted): the label-to-index mapping of each column, ordered by
  frequency in the training data. Zone has 25 labels (5, 3, 1 first, the busy zones),
  payment has 3 (card, app, cash), hour has 24 (18, 14, 7 first). Because of
  `handleInvalid="keep"`, an unseen value gets an extra index instead of an error.
- `OneHotEncoderModel` (fitted): the number of categories of each column, which fixes the
  vector sizes: 26, 4 and 25 including the "unknown" slot. With `dropLast`, the dropped
  slot is that unknown one.
- `VectorAssembler`: not fitted, a plain transformer. It learns nothing, it only puts the
  columns side by side.
- `LinearRegressionModel` (fitted): an intercept and 54 coefficients: 1.656 per km and 0.368
  per minute (slightly above the 1.6 and 0.35 of the generator, because of the surge
  hours). Since every real category has its own one-hot slot, the raw coefficients are
  large and only make sense as differences. Zone 5 (Airport) is +9.02 over zone 3, zone 21
  (Ferry) +3.98, zone 1 (Downtown) +2.52, cash +0.99 over card, and hour 8 +4.24 over 3 am.
  Those are the premiums hidden in `datagen.py` (9, 4, 2.5 and 1), found from the data.

---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?

Rides come into a Kafka topic. A long-running Structured Streaming job (on a cluster such as
EMR, Databricks or Spark on Kubernetes) reads it, scores every ride, writes all rides to the
data lake (S3, as Parquet or Delta), and writes the flagged ones to an `alerts` topic that
feeds the fraud team's queue. Its checkpoint lives on durable storage (S3 or HDFS), never on
a local disk, because the checkpoint holds the Kafka offsets the job has finished. The
training job is a nightly batch job (scheduled by Airflow, say). It reads the last 30 days of
completed trips from the lake, trains the pipeline as in part 2, and compares it with the
current model on a recent holdout. Only if it is better does it save a new versioned
PipelineModel, for example `s3://models/fare/2026-11-09/`, with its test RMSE (which sets the
threshold), and move a "production" pointer to it, a small file or an MLflow registry alias.
The scoring job picks up the new model in one of two ways. It can be stopped between
micro-batches and restarted: it loads the new model and resumes from the offsets in its
checkpoint, so no event is lost or skipped; Kafka simply holds them during the restart. Or it
can check the pointer inside `foreachBatch` and reload the model when it changes, with no
restart at all. A ride might be scored twice after a restart, so the alerts should be keyed
by `ride_id`. To know the model is going stale, I would watch the flag rate per window (a
few percent is normal; a steady climb means the prices moved, not that fraud did), the mean
and spread of `fare - prediction` per hour and zone on rides that were not flagged (it should
stay centered on 0 with a spread near the test RMSE), the share of rows that hit the
`handleInvalid="keep"` bucket (a new zone or payment type), how far the distance, hour and
zone mix drift from the training month, and the fraud team's confirm rate on alerts. For the
job itself I would watch Kafka consumer lag and batch duration.

---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

- **numpy missing in the Spark image.** The first run of part 1a failed before Spark started,
  on the `from pyspark.ml import Pipeline, PipelineModel` line of `main.py`:

  ```
  File "/opt/spark/python/lib/pyspark.zip/pyspark/ml/param/__init__.py", line 32, in <module>
  ModuleNotFoundError: No module named 'numpy'
  ```

  `pyspark.ml` needs numpy, and the `apache/spark:4.2.0` image does not include it. Fix:
  install it as root in the master and both workers (the `foreach` loop in the commands
  above; pip installed numpy 2.2.6). It has to be done again after `docker compose down`,
  since that removes the containers.

- **12 late rides dropped instead of 14.** Not an error, but it looked like one: I expected
  every 8-10 minute late event to be lost. Recomputing the rule by hand showed that Spark
  drops late rows using the previous batch's watermark and evicts windows using the current
  one, which explains R00739 and R00742 (see part 1a).
- **The truth file rewrites the data.** `python3 datagen.py <seed> shared-folder /tmp/truth.csv`
  regenerates every file in `shared-folder`, with new modification times. I wrote it to a
  separate folder instead (`$env:TEMP\a4truth`). Same seed, so it is the same rides.
- **Warnings that are not errors.** Part 2 prints
  `netlib-blas: JNI_OnLoad: dlopen(libblas.so.3) failed: libblas.so.3: cannot open shared
object file` (and the same for LAPACK). The image has no native BLAS, so Spark falls back
  to its Java implementation; the results are the same, only slower on big matrices. It
  also prints `regParam is zero, which might cause numerical instability`, which is expected
  for plain linear regression, and `Broadcasting large task binary` during the random
  forest fits, because the trees get large at depth 8 and 10. I left all three.
