"""Assignment #4: Ride-Sharing Analytics, from Stream to Model and Back.

Usage (on the Docker cluster from the hands-ons):
    spark-submit main.py <part> <shared directory> <seed>

    <part>              part1a, part1b, part2 or part3
    <shared directory>  the folder mounted in the containers, /opt/spark/work-dir/shared
    <seed>              your student ID (the split of part 2 and the forest, if you use one)

Layout of the shared directory:
    input/trips_history.csv, input/zones.csv     written by datagen.py (part 2)
    stream/rides_NN.jsonl                         written by datagen.py (parts 1 and 3)
    output/<part>/                                results of each part
    checkpoint/<part>/                            one checkpoint per streaming query
    models/fare                                   the PipelineModel saved by part 2

Each part is a function. A part that still returns None is reported as not implemented, so
the file always runs. Streaming parts use one file per micro-batch and the availableNow
trigger: they process the thirty files in order and stop. To run a streaming part again on
the same files, delete its checkpoint folder first.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, sum as ssum, avg, round as sround, window, hour,
                                   abs as sabs, lit)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

PARTS = ("part1a", "part1b", "part2", "part3")
if len(sys.argv) != 4 or sys.argv[1] not in PARTS:
    print(__doc__)
    sys.exit(2)
part, shared, seed = sys.argv[1], sys.argv[2].rstrip("/"), int(sys.argv[3])
in_dir, stream_dir = f"{shared}/input", f"{shared}/stream"
out_dir, ckpt_dir, model_dir = f"{shared}/output/{part}", f"{shared}/checkpoint/{part}", f"{shared}/models/fare"

spark = (SparkSession.builder.appName(f"A4-{part}")
         .config("spark.sql.shuffle.partitions", 4).getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# The stream of rides, as datagen.py writes it: one JSON object per line.
ride_schema = ("ride_id STRING, driver_id STRING, pickup_zone INT, distance_km DOUBLE, "
               "duration_min DOUBLE, payment_type STRING, fare DOUBLE, event_time TIMESTAMP")
# The historical trips. pickup_ts is a timestamp; fare is a clean double here (no A3 surprises).
trip_schema = ("trip_id STRING, driver_id STRING, pickup_zone INT, dropoff_zone INT, pickup_ts TIMESTAMP, "
               "distance_km DOUBLE, duration_min DOUBLE, payment_type STRING, fare DOUBLE")


def read_rides():
    """The streaming DataFrame of rides: one file per micro-batch, in order of arrival."""
    return (spark.readStream.schema(ride_schema)
            .option("maxFilesPerTrigger", 1).json(stream_dir))


def run(query, label):
    """Wait for a streaming query and print its micro-batch summary (input rows, watermark,
    state rows), the same numbers as the Structured Streaming tab of the Spark UI."""
    query.awaitTermination()
    print(f"\n=== {label}: micro-batch summary ===")
    print(f"{'batch':>5} {'input rows':>10} {'watermark':>20} {'state rows':>10}")
    for p in query.recentProgress:
        wm = p.get("eventTime", {}).get("watermark", "-")[:19].replace("T", " ")
        wm = "-" if wm.startswith("1970") else wm
        ops = p.get("stateOperators", [])
        print(f"{p['batchId']:>5} {p['numInputRows']:>10} {wm:>20} {ops[0]['numRowsTotal'] if ops else '-':>10}")


# ---------------------------------------------------------------- Part 1a: revenue per zone per window
def part1a_zone_windows():
    """Trips and revenue per pickup_zone per 5-minute tumbling window of event_time, with a
    3-minute watermark, written in APPEND mode to Parquet under out_dir with the columns
    window_start, window_end, pickup_zone, trips, revenue (sum of fare, 2 decimals).
    Trigger availableNow, checkpoint in ckpt_dir. Return the started query.
    After the query, main() prints what the stream wrote and the same aggregation computed
    in batch over all the files, so that you can see what the watermark did.
    """
    rides = read_rides()
    # TODO
    return None


# ---------------------------------------------------------------- Part 1b: alerts
def part1b_alerts():
    """A stateless alert stream: rides with fare > 150 or distance_km == 0, written as they
    arrive, in APPEND mode, to CSV with a header under out_dir, columns ride_id, event_time,
    pickup_zone, distance_km, duration_min, fare, reason, where reason is "high_fare" or
    "zero_distance" (a ride that is both gets "high_fare"). Use
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss"). Return the started query.
    """
    from pyspark.sql.functions import when
    rides = read_rides()
    # TODO
    return None


# ---------------------------------------------------------------- Part 2: the fare model
def features_of(df, ts_col):
    """The columns the models use, derived the same way for history and stream: hour of the
    trip as a string (a category, since the fare does not grow with the hour) and the zone
    as a string. Given: do not change it, part 3 relies on the same derivation."""
    return (df.withColumn("hour", hour(col(ts_col)).cast("string"))
              .withColumn("zone", col("pickup_zone").cast("string")))


def part2_train():
    """Train the fare model on input/trips_history.csv.

    1. Read the file with trip_schema (header), apply features_of(df, "pickup_ts"), split
       80/20 with randomSplit(seed=seed).
    2. baseline: VectorAssembler(["distance_km"]) -> LinearRegression(labelCol="fare").
    3. full: StringIndexer over ["zone", "payment_type", "hour"] (handleInvalid="keep")
       -> OneHotEncoder -> VectorAssembler(one-hot columns + ["distance_km", "duration_min"])
       -> LinearRegression(labelCol="fare").
    4. YOUR model: a third pipeline of your choice (RandomForestRegressor or GBTRegressor on
       the index columns, or the full pipeline with another label or regularization), tuned
       with ParamGridBuilder (at least two values of at least one parameter) and
       CrossValidator(numFolds=3, parallelism=2, seed=seed). Name it "tuned".
    5. Evaluate the three on the test set with RMSE and R2 (the evaluators below), print
       them, and save the PipelineModel with the lowest RMSE under model_dir with
       write().overwrite().save(). For the tuned model save cvModel.bestModel.
    Return the list of (name, rmse, r2); main writes it to output/part2/metrics.
    """
    rmse_eval = RegressionEvaluator(labelCol="fare", predictionCol="prediction", metricName="rmse")
    r2_eval = RegressionEvaluator(labelCol="fare", predictionCol="prediction", metricName="r2")
    trips = spark.read.csv(f"{in_dir}/trips_history.csv", header=True, schema=trip_schema)
    trips = features_of(trips, "pickup_ts")
    trainDF, testDF = trips.randomSplit([.8, .2], seed=seed)
    trainDF.cache()                                   # several fits read it: keep it in memory
    print(f"seed {seed}: {trainDF.count()} training rows, {testDF.count()} test rows")

    results, models = [], {}
    def evaluate(name, model):
        """Evaluate a fitted PipelineModel on the test set and remember it."""
        pred = model.transform(testDF)
        rmse, r2 = rmse_eval.evaluate(pred), r2_eval.evaluate(pred)
        print(f"{name:10s} RMSE {rmse:6.2f}   R2 {r2:6.3f}")
        results.append((name, round(rmse, 2), round(r2, 3))); models[name] = model

    # TODO: baseline, full, tuned (with ParamGridBuilder and CrossValidator); evaluate(...)
    #       each; save models[<best name>] under model_dir; return results
    return None


# ---------------------------------------------------------------- Part 3: score the stream
def part3_score(threshold):
    """The scoring job. Load the PipelineModel from model_dir, apply features_of(rides,
    "event_time") to the stream and transform it. Add deviation = |fare - prediction|
    (2 decimals) and keep the rides with deviation > threshold. Write them in APPEND mode
    to CSV with a header under out_dir, columns ride_id, event_time, pickup_zone,
    distance_km, duration_min, payment_type, fare, prediction (2 decimals), deviation,
    with the timestampFormat option of part 1b. Trigger availableNow, checkpoint in
    ckpt_dir. Return the started query. No fit() anywhere in this function.
    """
    # TODO
    return None


# ---------------------------------------------------------------- main
def metrics_of_saved_model():
    """The RMSE of the model part 2 saved, from output/part2/metrics (the row with the
    lowest rmse). Part 3 flags a ride when its deviation exceeds 4 times that RMSE."""
    m = spark.read.csv(f"{shared}/output/part2/metrics", header=True, inferSchema=True)
    best = m.orderBy("rmse").first()
    return best["model"], float(best["rmse"])


if part == "part1a":
    q = part1a_zone_windows()
    if q is not None:
        run(q, part)
        print("\n=== part1a: windows written by the stream ===")
        spark.read.parquet(out_dir).orderBy("window_start", "pickup_zone").show(100, truncate=False)
        print("=== part1a: the same aggregation in batch over all files ===")
        (spark.read.schema(ride_schema).json(stream_dir)
         .groupBy(window(col("event_time"), "5 minutes"), "pickup_zone")
         .agg(count("*").alias("trips"), sround(ssum("fare"), 2).alias("revenue"))
         .select(col("window.start").alias("window_start"), "pickup_zone", "trips", "revenue")
         .orderBy("window_start", "pickup_zone").show(100, truncate=False))
elif part == "part1b":
    q = part1b_alerts()
    if q is not None:
        run(q, part)
        alerts = spark.read.csv(out_dir, header=True)
        print(f"\n=== part1b: {alerts.count()} alerts ==="); alerts.groupBy("reason").count().show()
elif part == "part2":
    result = part2_train()
    q = None if result is None else True
    if result:
        (spark.createDataFrame(result, ["model", "rmse", "r2"]).coalesce(1)
         .write.mode("overwrite").option("header", True).csv(f"{out_dir}/metrics"))
else:
    name, rmse = metrics_of_saved_model()
    threshold = round(4 * rmse, 2)
    print(f"saved model: {name}, test RMSE {rmse}; flagging rides with deviation > {threshold}")
    q = part3_score(threshold)
    if q is not None:
        run(q, part)
        flagged = spark.read.csv(out_dir, header=True, inferSchema=True)
        print(f"\n=== part3: {flagged.count()} rides flagged ===")
        flagged.orderBy(col("deviation").desc()).show(10, truncate=False)
        print("=== part3: flagged rides per 5-minute window (batch over the output) ===")
        (flagged.groupBy(window(col("event_time"), "5 minutes")).count()
         .select(col("window.start").alias("window_start"), "count").orderBy("window_start").show())

if q is None:
    print(f"\n=== {part}: not implemented yet ===")
else:
    print(f"\n=== {part} done; results under {out_dir} ===")
spark.stop()
