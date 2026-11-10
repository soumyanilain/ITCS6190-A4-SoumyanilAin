"""Assignment #4: data generator (plain Python, standard library only).

    python3 datagen.py <seed> [shared directory]

Writes, under the shared directory (default: shared-folder next to this file):

  input/trips_history.csv   60,000 completed trips of September 2026: the training data
  input/zones.csv           the 25 zones of the city, with name and borough
  stream/rides_00.jsonl ... rides_29.jsonl
                            thirty minutes of live rides on October 27, 2026, one file per
                            minute of ARRIVAL, one JSON object per line: the stream

The fare of a trip depends on its distance, its duration, the pickup zone, the hour and
the payment type, plus some noise: there is something to learn. In the stream, a few
percent of the rides carry a fare that does not match the trip (far too high or far too
low): the anomalies that part 3 must find. Some stream events are late (their event_time
is one to two minutes before the file's minute) and a few are very late (eight to ten
minutes); a few rides have a distance of zero.

The seed makes the data yours and reproducible: the grader regenerates it from the seed you
write in your report. The stream files get modification times one minute apart, in order,
because Spark's file source reads new files oldest first: generate them where Spark reads
them and do not copy them around.
"""
import csv, json, math, os, random, sys, time
from datetime import datetime, timedelta

if len(sys.argv) < 2:
    print(__doc__); sys.exit(2)
seed = int(sys.argv[1])
shared = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared-folder")
truth_path = sys.argv[3] if len(sys.argv) > 3 else None      # grader only: where to write the planted anomalies
os.makedirs(f"{shared}/input", exist_ok=True); os.makedirs(f"{shared}/stream", exist_ok=True)
rnd = random.Random(seed)

# ---------------------------------------------------------------- zones
names = ["Downtown", "Uptown", "Midtown", "Harbor", "Airport", "University", "Old Town", "Riverside",
         "Tech Park", "Stadium", "Market", "Hillside", "Lakeview", "Eastgate", "Westgate", "Northfield",
         "Southport", "Greenwood", "Civic Center", "Arts District", "Ferry", "Parkside", "Mill", "Heights", "Bayfront"]
boroughs = ["Central", "North", "South", "East", "West"]
zones = [{"zone_id": i + 1, "zone_name": n, "borough": boroughs[i % 5]} for i, n in enumerate(names)]
with open(f"{shared}/input/zones.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["zone_id", "zone_name", "borough"]); w.writeheader(); w.writerows(zones)

# the hidden pricing model: base fare, per km, per minute, a premium per zone, surge at rush hour,
# a small surcharge for cash. Models in part 2 have to rediscover it from the data.
zone_premium = {z["zone_id"]: 0.0 for z in zones}
zone_premium[5] = 9.0          # Airport
zone_premium[1] = 2.5          # Downtown
zone_premium[21] = 4.0         # Ferry
pay_extra = {"card": 0.0, "app": 0.0, "cash": 1.0}
zone_weights = [6 if z["zone_id"] in (1, 3, 5) else 1 + (z["zone_id"] % 3) for z in zones]

def fare_of(distance, duration, zone, hour, payment):
    surge = 1.25 if hour in (7, 8, 17, 18) else 1.0
    return (2.5 + 1.6 * distance + 0.35 * duration) * surge + zone_premium[zone] + pay_extra[payment]

def trip(ts):
    zone = rnd.choices(zones, weights=zone_weights, k=1)[0]["zone_id"]
    distance = round(max(0.3, rnd.lognormvariate(1.4, 0.6)), 2)          # median about 4 km
    duration = round(distance * rnd.uniform(2.2, 4.0) + rnd.uniform(1, 4), 1)
    payment = rnd.choices(["card", "app", "cash"], weights=[5, 4, 1], k=1)[0]
    fare = round(fare_of(distance, duration, zone, ts.hour, payment) + rnd.gauss(0, 1.5), 2)
    return zone, distance, duration, payment, max(3.0, fare)

# ---------------------------------------------------------------- history
N_TRIPS, N_DRIVERS = 60000, 250
base = datetime(2026, 9, 1)
with open(f"{shared}/input/trips_history.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["trip_id", "driver_id", "pickup_zone", "dropoff_zone", "pickup_ts", "distance_km",
                "duration_min", "payment_type", "fare"])
    for i in range(1, N_TRIPS + 1):
        ts = base + timedelta(seconds=rnd.randint(0, 30 * 24 * 3600 - 1))
        zone, distance, duration, payment, fare = trip(ts)
        w.writerow([f"T{i:06d}", f"D{rnd.randint(1, N_DRIVERS):03d}", zone, rnd.randint(1, 25),
                    ts.strftime("%Y-%m-%d %H:%M:%S"), distance, duration, payment, fare])

# ---------------------------------------------------------------- stream
N_FILES = 30
start = datetime(2026, 10, 27, 8, 0, 0)
ride = 0; truth = []; n_late = n_very_late = n_zero = 0
for k in range(N_FILES):
    minute = start + timedelta(minutes=k)
    rows = []
    for _ in range(rnd.randint(35, 45)):
        ride += 1
        arrival = minute + timedelta(seconds=rnd.randint(0, 59))
        r = rnd.random()
        if k >= 2 and r < 0.06:
            event = arrival - timedelta(seconds=rnd.randint(60, 120)); n_late += 1
        elif k >= 12 and r < 0.09:
            event = arrival - timedelta(seconds=rnd.randint(480, 600)); n_very_late += 1
        else:
            event = arrival - timedelta(seconds=rnd.randint(0, 30))
        zone, distance, duration, payment, fare = trip(event)
        kind = "ok"
        a = rnd.random()
        if a < 0.02:
            fare = round(fare * rnd.uniform(2.2, 3.0), 2); kind = "high"
        elif a < 0.035:
            fare = round(fare * rnd.uniform(0.2, 0.4), 2); kind = "low"
        elif a < 0.045:
            distance = 0.0; n_zero += 1                                     # a ride that never moved
        rid = f"R{ride:05d}"
        if kind != "ok": truth.append((rid, kind))
        rows.append({"ride_id": rid, "driver_id": f"D{rnd.randint(1, N_DRIVERS):03d}", "pickup_zone": zone,
                     "distance_km": distance, "duration_min": duration, "payment_type": payment,
                     "fare": fare, "event_time": event.strftime("%Y-%m-%d %H:%M:%S")})
    with open(f"{shared}/stream/rides_{k:02d}.jsonl", "w") as f:
        for row in rows: f.write(json.dumps(row) + "\n")
now = time.time()
for k in range(N_FILES):
    t = now - 60 * (N_FILES - k)
    os.utime(f"{shared}/stream/rides_{k:02d}.jsonl", (t, t))
if truth_path:
    with open(truth_path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["ride_id", "kind"]); w.writerows(truth)
print(f"seed {seed}: {N_TRIPS} historical trips, {N_FILES} stream files with {ride} rides "
      f"({len(truth)} fares that do not match the trip, {n_late} late by 1-2 minutes, "
      f"{n_very_late} late by 8-10 minutes, {n_zero} with zero distance)")
