import argparse
import logging
import os
import re
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cli import resolve_path_arg

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# This module runs the collision event analysis. It loads the preprocessed parquet data,
# filters for non-stationary vessels, removes points with GPS anomalies, detects proximity events and
# extracts the trajectories of both involved vessels in the 20 minute time window around the event.

# Thresholds and parameters for filtering:
MOVING_SOG_THRESHOLD_KNOTS = 0.5
MAX_REASONABLE_SPEED_KNOTS = 40.0
PROXIMITY_THRESHOLD_NM = 0.1
TIME_BUCKET_SECONDS = 60
CELL_SIZE_DEG = max(0.004, PROXIMITY_THRESHOLD_NM / 60 * 2) # ~0.004 degrees is about 0.24 NM, so this creates a grid that helps limit pairwise comparisons to nearby points.
MIN_DISTANCE_NM = 0.01
STATIONARY_LABELS = ["At anchor", "Moored", "Aground"]
IGNORED_SHIP_TYPES = ["Tug", "Towing", "SAR", "Port tender","Law enforcement"]


def build_parser() -> argparse.ArgumentParser:
	"""Build the CLI argument parser specifically for the collision analysis script."""
	parser = argparse.ArgumentParser(description="Detect vessel collision/proximity events.")
	parser.add_argument("--parquet-dir", dest="parquet_dir", type=Path, help="Directory containing preprocessed parquet files.")
	return parser


def haversine_nm_expr(lat1_col: str, lon1_col: str, lat2_col: str, lon2_col: str):
	"""Calculate geographical distance between two points in nautical miles."""
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
		.getOrCreate()
	)


def load_preprocessed_data(spark: SparkSession, parquet_dir: Path) -> DataFrame:
	"""Load December parquet output from preprocessing step."""
	return spark.read.parquet(str(parquet_dir / "aisdk-2021-12-*.parquet"))


def filter_stationary_vessels(df: DataFrame, sog_threshold: float = MOVING_SOG_THRESHOLD_KNOTS) -> DataFrame:
	"""Keep likely moving vessels only. Filters out points with low SOG and stationary navigational status."""
	# Calculate 50th percentile SOG threshold per vessel. 
	# This is done to include vessels that may be slow-moving but still in motion.
	# Otherwise, slow vessels would be lost.
	stat_sog = (
		df.groupBy("mmsi")
		.agg(F.percentile_approx("sog", 0.5).alias("stat_sog"))
	)
	return (
		df.join(stat_sog, on="mmsi", how="left")
		.filter(
			(F.col("stat_sog") >= F.lit(sog_threshold))
			& (~F.col("navigational_status").isin(STATIONARY_LABELS))
		)
		.drop("stat_sog")
	)


def filter_ignored_ship_types(df: DataFrame, ignored_ship_types: list[str] = IGNORED_SHIP_TYPES) -> DataFrame:
	"""Exclude vessels whose ship_type text matches any configured ignored types."""
	if "ship_type" not in df.columns or not ignored_ship_types:
		return df

	# Build a regex pattern to match any of the ignored ship types, ignoring case and potentially null values.
	escaped = "|".join(re.escape(v) for v in ignored_ship_types)

	return df.filter(~F.lower(F.coalesce(F.col("ship_type").cast("string"), F.lit(""))).rlike(escaped.lower()))


def filter_gps_anomalies(
	df: DataFrame,
	max_speed_knots: float = MAX_REASONABLE_SPEED_KNOTS,
	min_points: int = 3,
) -> DataFrame:
	"""Remove implausible jumps using per-vessel point-to-point implied speed."""
	# 1. Use the window function to calculate the previous ping's latitude and longitude.
	w = Window.partitionBy("mmsi").orderBy("timestamp")

	with_prev = (
		df.withColumn("prev_latitude", F.lag("latitude").over(w))
		.withColumn("prev_longitude", F.lag("longitude").over(w))
		.withColumn("prev_timestamp", F.lag("timestamp").over(w))
	)
	# 2. Calculate the time delta, distance delta and the implied speed between consecutive points.
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
	# 3. Filter out impossible speeds and keep only vessels with at least some valid points after filtering.
	point_counts = with_delta.groupBy("mmsi").agg(F.count("*").alias("point_count"))

	clean = (
		with_delta.filter(
			F.col("prev_timestamp").isNull() | (F.col("implied_speed_knots") <= F.lit(max_speed_knots))
		)
		.join(point_counts, on="mmsi")
		.filter(F.col("point_count") >= F.lit(min_points))
		.drop("point_count")
	)
	# 4. Drop intermediate columns before returning the cleaned dataframe.
	return clean.drop(
		"prev_latitude",
		"prev_longitude",
		"prev_timestamp",
		"delta_seconds",
		"step_distance_nm",
		"implied_speed_knots",
	)


def detect_collision_candidates(
	df: DataFrame,
	proximity_nm: float = PROXIMITY_THRESHOLD_NM,
) -> DataFrame:
	"""Build pairwise proximity events using time buckets."""
	# 1. Assign each point to a time bucket and spatial cell. This is done to limit the number of pairwise comparisons.
	bucketed = df.withColumn(
		"time_bucket",
		(
			F.floor(F.col("timestamp").cast("long") / F.lit(TIME_BUCKET_SECONDS))
			* F.lit(TIME_BUCKET_SECONDS)
		).cast("long"),
	)
	# 2. Create spatial cells by bucketing latitude and longitude.
	bucketed = bucketed.withColumn("lat_cell", F.floor(F.col("latitude") / F.lit(CELL_SIZE_DEG)))
	bucketed = bucketed.withColumn("lon_cell", F.floor(F.col("longitude") / F.lit(CELL_SIZE_DEG)))
	bucketed = (
		bucketed.groupBy("mmsi", "time_bucket")
		.agg(
			F.first("timestamp").alias("timestamp"),
			F.first("latitude").alias("latitude"),
			F.first("longitude").alias("longitude"),
			F.first("name").alias("name"),
			F.first("sog").alias("sog"),
			F.first("lat_cell").alias("lat_cell"),
			F.first("lon_cell").alias("lon_cell"),
		)
		.cache()
	)
	# 3. Self join the bucketed df to find pairs that are in the same bucket and spatial cell. Calculate the distance and filter by the threshold.
	a = bucketed.alias("a")
	b = bucketed.alias("b")

	paired = a.join(
		b,
		on=[
			F.col("a.time_bucket") == F.col("b.time_bucket"),
			F.col("a.lat_cell") == F.col("b.lat_cell"),
			F.col("a.lon_cell") == F.col("b.lon_cell"),
			F.col("a.mmsi") < F.col("b.mmsi"),
		],
		how="inner",
	)
	# 4. Additionally, apply a bounding box to filter out far apart points.
	paired = paired.filter(
		(F.abs(F.col("a.latitude") - F.col("b.latitude")) <= F.lit(0.002))
		& (F.abs(F.col("a.longitude") - F.col("b.longitude")) <= F.lit(0.002))
	)
	# 5. Calculate the precise haversine distance for the remaining pairs and filter by the proximity threshold.
	# Also, calculate the event timestamp and location as the middle point for the map.
	paired = (
		paired.withColumn(
			"pair_distance_nm",
			haversine_nm_expr(
				"a.latitude",
				"a.longitude",
				"b.latitude",
				"b.longitude",
			),
		)
		.filter(
			(F.col("pair_distance_nm") <= F.lit(proximity_nm))
			& (F.col("pair_distance_nm") >= F.lit(MIN_DISTANCE_NM))
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
		.withColumn("event_longitude", (F.col("longitude_1") + F.col("longitude_2")) / F.lit(2.0))
		.cache()
	)
	# 6. Keep pairs with at least some valid points after filtering.
	return paired


def choose_primary_event(candidates: DataFrame) -> DataFrame:
	"""Choose the primary event from the candidates."""
	# 1. Group vessel pairs and count the number of events, track minimum distance.
	encounters = (
		candidates.groupBy("mmsi_1", "mmsi_2", "name_1", "name_2")
		.agg(
			F.count("*").alias("n_events"),
			F.min("pair_distance_nm").alias("min_distance"),
		)
	)
	# 2. For insight purposes, log the top pairs by minimum distance.
	log.info("Top encounter pairs:")
	encounters.orderBy(F.col("min_distance").asc(),F.col("n_events").asc()).show(20, truncate=False)
	# 3. The smallest distance = the most likely proximity event in this solution.
	best_pair = (
		encounters.orderBy(F.col("min_distance").asc(),F.col("n_events").desc())
		.first()
	)
	# 4. Return the event details for the best pair.
	return (
		candidates.filter(
			(F.col("mmsi_1") == best_pair["mmsi_1"])
			& (F.col("mmsi_2") == best_pair["mmsi_2"])
		)
		.orderBy(F.col("pair_distance_nm").asc())
		.limit(1)
	)



def extract_event_trajectory(df: DataFrame, event_row) -> DataFrame:
	"""Extract the +-10 minute trajectory for both vessels around the detected event."""
	# 1. Get event timestamp and MMSIs from the event row.
	start_ts = event_row["event_timestamp"]
	mmsi_1 = event_row["mmsi_1"]
	mmsi_2 = event_row["mmsi_2"]
	# 2. Filter the original df for points belonging to the trajectory window and the two vessels.
	# Order by timestamp for the map visualization.
	return (
		df.filter(F.col("mmsi").isin([mmsi_1, mmsi_2]))
		.filter(
			(F.col("timestamp") >= F.lit(start_ts) - F.expr("INTERVAL 10 MINUTES"))
			& (F.col("timestamp") <= F.lit(start_ts) + F.expr("INTERVAL 10 MINUTES"))
		)
		.select("timestamp", "mmsi", "name", "latitude", "longitude", "sog")
		.orderBy("timestamp", "mmsi")
	)


def run_collision_analysis(parquet_dir: Path):
	"""Run the collision/proximity analysis pipeline and return event details plus trajectories."""
	# 1. Build Spark session and load preprocessed data.
	spark = build_spark()

	try:
		base_df = load_preprocessed_data(spark, parquet_dir)
		log.info("Rows loaded: %s", f"{base_df.count():,}")

		if "name" not in base_df.columns:
			base_df = base_df.withColumn("name", F.lit(None))
		# 2. Filter out ignored ship types.
		base_df = filter_ignored_ship_types(base_df)
		log.info("Rows after ship-type exclusion: %s", f"{base_df.count():,}")
		# 3. Filter out stationary vessels.
		moving_df = filter_stationary_vessels(base_df)
		log.info("Moving rows: %s", f"{moving_df.count():,}")
		# 4. Filter out GPS anomalies.
		denoised_df = filter_gps_anomalies(moving_df)
		log.info("Denoised rows: %s", f"{denoised_df.count():,}")
		# 5. Detect proximity events and get the candidates.
		candidates = detect_collision_candidates(denoised_df)
		first_candidate = candidates.first()
		# 6. If none found, return an error message.
		if first_candidate is None:
			return None, None, "No collision/proximity events found."
		# 7. Otherwise, choose the primary event and extract the trajectories for the map.
		event_df = choose_primary_event(candidates)
		event_row = event_df.collect()[0]
		trajectory_df = extract_event_trajectory(denoised_df, event_row)
		trajectory_pdf = trajectory_df.toPandas()
		# 8. Build the dictionary of event details to return.
		result = {
			"mmsi_1": int(event_row["mmsi_1"]),
			"name_1": event_row["name_1"],
			"mmsi_2": int(event_row["mmsi_2"]),
			"name_2": event_row["name_2"],
			"event_timestamp": str(event_row["event_timestamp"]),
			"event_latitude": float(event_row["event_latitude"]),
			"event_longitude": float(event_row["event_longitude"]),
			"pair_distance_nm": float(event_row["pair_distance_nm"]),
		}
		# 9. Return the event details and the trajectory df for map visualization. Stop Spark session.
		return result, trajectory_pdf, None
	finally:
		spark.stop()


if __name__ == "__main__":
	args = build_parser().parse_args()
	base_dir = Path(__file__).resolve().parent
	parquet_path = resolve_path_arg(args.parquet_dir, "AIS_PARQUET_DIR", base_dir / "Data" / "Parquet", base_dir)
	event, _, err = run_collision_analysis(parquet_path)

	if err:
		print(err)
	else:
		print("Detected collision/proximity event:")
		print(event)
