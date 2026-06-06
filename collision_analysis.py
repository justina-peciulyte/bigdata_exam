import argparse
from pathlib import Path
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from cli import resolve_path_arg

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# This module runs the collision event analysis. It loads the preprocessed parquet data, 
# filters for non stationary vessels, removes points with GPS anomalies, detects proximity events and
# extracts the trajectories of both involved vessels in the 20 min. time window around the event.

# Thresholds and parameters for filtering:
MOVING_SOG_THRESHOLD_KNOTS = 0.5
MAX_REASONABLE_SPEED_KNOTS = 60.0
PROXIMITY_THRESHOLD_NM = 0.25
TIME_BUCKET_SECONDS = 30
CELL_SIZE_DEG = 0.01


def build_parser() -> argparse.ArgumentParser:
	"""Builds the CLI argument parser specifically for the collision analysis script."""
	parser = argparse.ArgumentParser(description="Detect vessel collision/proximity events.")
	parser.add_argument("--parquet-dir", dest="parquet_dir", type=Path, help="Directory containing preprocessed parquet files.")
	return parser


def haversine_nm_expr(lat1_col: str, lon1_col: str, lat2_col: str, lon2_col: str):
	"""Calculates geographical distance between two points in nautical miles."""
	earth_radius_km = 6371.0088
	km_to_nm = 0.539956803

	lat1 = F.radians(F.col(lat1_col))
	lon1 = F.radians(F.col(lon1_col))
	lat2 = F.radians(F.col(lat2_col))
	lon2 = F.radians(F.col(lon2_col))

	dlat = lat2 - lat1
	dlon = lon2 - lon1
	a = F.pow(F.sin(dlat / 2.0), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2.0), 2)
	c = 2.0 * F.atan2(F.sqrt(a), F.sqrt(1.0 - a))

	return c * F.lit(earth_radius_km * km_to_nm)


def build_spark() -> SparkSession:
    """SparkSession builder with optimized settings."""
    cores = os.cpu_count() or 1
    log.info("Detected %d CPU core(s) available to the container", cores)

    return (
        SparkSession.builder
        .appName("AIS-Collision-Detector")
        .master(f"local[{cores}]")
        .config("spark.driver.cores", cores)
        .config("spark.default.parallelism", cores * 2)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", cores * 2)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.ui.enabled", "false")
        .getOrCreate())


def load_preprocessed_data(spark: SparkSession, parquet_dir: Path) -> DataFrame:
	"""Load December parquet output from preprocessing step."""
	return spark.read.parquet(str(parquet_dir / "aisdk-2021-12-*.parquet"))


def filter_stationary_vessels(df: DataFrame, sog_threshold: float = MOVING_SOG_THRESHOLD_KNOTS) -> DataFrame:
	"""Keep likely moving vessels only."""
	return df.filter(F.col("sog") > F.lit(sog_threshold))


def filter_gps_anomalies(df: DataFrame, max_speed_knots: float = MAX_REASONABLE_SPEED_KNOTS) -> DataFrame:
	"""Removes implausible jumps using per-vessel point-to-point implied speed."""
	w = Window.partitionBy("mmsi").orderBy("timestamp")

	with_prev = (
		df.withColumn("prev_latitude", F.lag("latitude").over(w))
		.withColumn("prev_longitude", F.lag("longitude").over(w))
		.withColumn("prev_timestamp", F.lag("timestamp").over(w))
	)

	with_delta = (
		with_prev.withColumn(
			"delta_seconds",
			(F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")).cast("double"),
		)
		.withColumn(
			"step_distance_nm",
			F.when(
				F.col("prev_latitude").isNotNull(),
				haversine_nm_expr("prev_latitude", "prev_longitude", "latitude", "longitude"),
			).otherwise(F.lit(None)),
		)
		.withColumn(
			"implied_speed_knots",
			F.when(
				F.col("delta_seconds") > F.lit(0),
				F.col("step_distance_nm") / (F.col("delta_seconds") / F.lit(3600.0)),
			).otherwise(F.lit(None)),
		)
	)

	# Keep first point per vessel and points with plausible movement.
	return with_delta.filter(
		F.col("prev_timestamp").isNull() | (F.col("implied_speed_knots") <= F.lit(max_speed_knots))
	).drop("prev_latitude", "prev_longitude", "prev_timestamp", "delta_seconds", "step_distance_nm", "implied_speed_knots")


def detect_collision_candidates(df: DataFrame, proximity_nm: float = PROXIMITY_THRESHOLD_NM) -> DataFrame:
	"""Build pairwise proximity events using time buckets."""

	# 1. Create time buckets for joining many points together that are close in time. 
	bucketed = (df.withColumn("time_bucket",
						   (F.floor(F.col("timestamp").cast("long") / 
				  F.lit(TIME_BUCKET_SECONDS))
				  * F.lit(TIME_BUCKET_SECONDS)).cast("long")))
	
	bucketed = (bucketed.withColumn("lat_cell",F.floor(F.col("latitude") / 
				 F.lit(CELL_SIZE_DEG))).withColumn("lon_cell",F.floor(F.col("longitude") / F.lit(CELL_SIZE_DEG))))
	bucketed = (
		bucketed
		.groupBy("mmsi", "time_bucket")
		.agg(
			F.first("timestamp").alias("timestamp"),
        	F.first("latitude").alias("latitude"),
        	F.first("longitude").alias("longitude"),
        	F.first("name").alias("name"),
        	F.first("sog").alias("sog"),
        	F.first("lat_cell").alias("lat_cell"),
        	F.first("lon_cell").alias("lon_cell"),
		)
	)
	# Cache the more expensive intermediate dataframe and use it later.
	bucketed = bucketed.cache()

	a = bucketed.alias("a")
	b = bucketed.alias("b")

	# 2. Get candidate pairs by joining on time buckets and filtering by proximity.
	
	paired = (
		a.join(b, on=[
			F.col("a.time_bucket") == F.col("b.time_bucket"),
            F.col("a.lat_cell") == F.col("b.lat_cell"),
			F.col("a.lon_cell") == F.col("b.lon_cell"),
			F.col("a.mmsi") < F.col("b.mmsi"),],
        how="inner",))
	
	paired = (
		paired.filter(
			F.abs(
				F.col("a.latitude") - F.col("b.latitude")) <= F.lit(0.01))
			.filter(
				F.abs(
					F.col("a.longitude") - F.col("b.longitude")) <= F.lit(0.01)))
	paired = (
		paired
		.withColumn(
			"pair_distance_nm",
			haversine_nm_expr(
				"a.latitude",
				"a.longitude",
				"b.latitude",
				"b.longitude",
			),
		)
		.filter(
			F.col("pair_distance_nm") <= F.lit(proximity_nm)
		)
		.select(
			F.col("a.mmsi").alias("mmsi_1"),
			F.col("a.name").alias("name_1"),
			F.col("a.timestamp").alias("timestamp_1"),
			F.col("a.latitude").alias("latitude_1"),
			F.col("a.longitude").alias("longitude_1"),
			F.col("b.mmsi").alias("mmsi_2"),
			F.col("b.name").alias("name_2"),
			F.col("b.timestamp").alias("timestamp_2"),
			F.col("b.latitude").alias("latitude_2"),
			F.col("b.longitude").alias("longitude_2"),
			F.col("pair_distance_nm"),
		)
		.withColumn(
			"event_timestamp",
			F.from_unixtime(
				(
					(F.col("timestamp_1").cast("long") + F.col("timestamp_2").cast("long"))
					/ F.lit(2)
				).cast("long")
			).cast("timestamp"),
		)
		.withColumn("event_latitude", (F.col("latitude_1") + F.col("latitude_2")) / F.lit(2.0))
		.withColumn("event_longitude", (F.col("longitude_1") + F.col("longitude_2")) / F.lit(2.0)))
	
	# Cache the paired dataframe to use later.
	paired = paired.cache()

	# 3. The resulting dataframe has one row per pair of points with event details.
	return paired


def choose_primary_event(candidates: DataFrame) -> DataFrame:
	"""Picks the closest encounter as primary event."""
	# In case of multiple candidates, they are ordered by distance and time, and the first one is selected.
	return candidates.orderBy(F.col("pair_distance_nm").asc(), F.col("event_timestamp").asc()).limit(1)


def extract_event_trajectory(df: DataFrame, event_row) -> DataFrame:
	"""Extracts the +-10 minute trajectory for both vessels around detected event."""

	# 1. Take event timestamp and vessel MMSI's from the event row in data.
	start_ts = event_row["event_timestamp"]
	mmsi_1 = event_row["mmsi_1"]
	mmsi_2 = event_row["mmsi_2"]

	# 2. Filter the main dataframe to find the track of both vessels in the 10 minute window.
	# Returns the resulting filtered dataframe with only relevant columns ordered by time and vessels.
	return (
		df.filter(F.col("mmsi").isin([mmsi_1, mmsi_2]))
		.filter(
			(F.col("timestamp") >= F.lit(start_ts) - F.expr("INTERVAL 10 MINUTES"))
			& (F.col("timestamp") <= F.lit(start_ts) + F.expr("INTERVAL 10 MINUTES")))
		.select("timestamp", "mmsi", "name", "latitude", "longitude", "sog")
		.orderBy("timestamp", "mmsi"))


def run_collision_analysis(parquet_dir: Path):
	"""Runs the collision/proxity analysis pipeline. Returns the detected event and trajectories."""

	# 1. Define Spark session for big data processing.
	spark = build_spark()
	
	# 2. Load preprocessed data, filter for moving vessels and remove GPS anomalies.
	base_df = load_preprocessed_data(spark, parquet_dir)

	print(f"Rows loaded: {base_df.count():,}")

	# Ensure `name` column exists (preprocessing currently writes `name`).
	if "name" not in base_df.columns:
		base_df = base_df.withColumn("name", F.lit(None))
	moving_df = filter_stationary_vessels(base_df)
	print(f"Moving rows: {moving_df.count():,}")
	denoised_df = filter_gps_anomalies(moving_df)
	print(f"Denoised rows: {denoised_df.count():,}")
	candidates = detect_collision_candidates(denoised_df)
	
	# 3. Either return no events foeund or pick the primary event and extract trajectories.
    # If no candidate events are found, return a message and stop session.
	first_candidate = candidates.first()
	if first_candidate is None:
		return None, None, "No collision/proximity events found."
    # Otherwise pick the latest event and extract trajectories of both vessels.
	event_df = choose_primary_event(candidates)
	event_row = event_df.collect()[0]
	trajectory_df = extract_event_trajectory(denoised_df, event_row)
	trajectory_pdf = trajectory_df.toPandas()
	print(trajectory_pdf.groupby("mmsi").size())
	print(trajectory_pdf[["timestamp", "mmsi", "latitude", "longitude"]].head(50))

	# 4. Make a result dictionary for event details.
	result = {"mmsi_1": int(event_row["mmsi_1"]),
		   "name_1": event_row["name_1"],
		   "mmsi_2": int(event_row["mmsi_2"]),
		   "name_2": event_row["name_2"],
		   "event_timestamp": str(event_row["event_timestamp"]),
		   "event_latitude": float(event_row["event_latitude"]),
		   "event_longitude": float(event_row["event_longitude"]),
		   "pair_distance_nm": float(event_row["pair_distance_nm"]),}
	
	# 5. Stop Spark session and return the result.
	spark.stop()
	return result, trajectory_pdf, None


if __name__ == "__main__":
	# Run the collision analysis and print the detected event details.
	args = build_parser().parse_args()
	base_dir = Path(__file__).resolve().parent
	parquet_path = resolve_path_arg(args.parquet_dir, "AIS_PARQUET_DIR", base_dir / "Data" / "Parquet", base_dir)
	event, _, err = run_collision_analysis(parquet_path)

	if err:
		print(err)
	else:
		print("Detected collision/proximity event:")
		print(event)
