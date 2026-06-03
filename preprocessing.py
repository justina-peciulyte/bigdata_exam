from pathlib import Path
import math
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Input and output paths:
INPUT_DIR = Path("Data/Extracted")
OUTPUT_DIR = Path("Data/Parquet")

# Geografical bounds for filtering vessels:
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM = 50.0


def normalize_col_name(col_name: str) -> str:
    """Normalizes column names to lowercase, no whitespace or special characters."""
    cleaned = col_name.strip().lower()
    cleaned = cleaned.replace("#", "")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def haversine_nm_expr(lat_col: str, lon_col: str, center_lat: float, center_lon: float):
    """Returns pyspark expression for haversine distance in nautical miles."""
    earth_radius_km = 6371.0088
    km_to_nm = 0.539956803
    c_lat = math.radians(center_lat)
    c_lon = math.radians(center_lon)

    lat_r = F.radians(F.col(lat_col))
    lon_r = F.radians(F.col(lon_col))
    dlat = lat_r - F.lit(c_lat)
    dlon = lon_r - F.lit(c_lon)

    a = F.pow(F.sin(dlat / 2.0), 2) + F.cos(lat_r) * F.lit(math.cos(c_lat)) * F.pow(F.sin(dlon / 2.0), 2)
    c = 2.0 * F.atan2(F.sqrt(a), F.sqrt(1.0 - a))
    return c * F.lit(earth_radius_km * km_to_nm)


def preprocess_daily_file(spark: SparkSession, csv_file: Path, output_dir: Path) -> None:
    """Preprocesses a single daily CSV file and saves it to a parquet file."""
    print(f"Processing file: {csv_file.name}")

    raw_df = (spark.read.option("header", True)
              .option("multiLine", False)
              .option("mode", "PERMISSIVE")
              .csv(str(csv_file)))

    # Normalize all column names at once.
    normalized_cols = [normalize_col_name(c) for c in raw_df.columns]
    df = raw_df.toDF(*normalized_cols)

    # Check for required columns in collision analysis and parse data types.
    required = ["timestamp", "mmsi", "latitude", "longitude"]
    # IF any are missing, raise an error and skip the file.
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {csv_file.name}: {missing}")

    parsed = (df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss"))
              .withColumn("mmsi", F.col("mmsi").cast("long"))
              .withColumn("latitude", F.col("latitude").cast("double"))
              .withColumn("longitude", F.col("longitude").cast("double"))
              .withColumn("sog", F.col("sog").cast("double")))

    # Keep only fields needed by later collision analysis and drop others for efficiency.
    keep_columns = ["timestamp",
                    "mmsi",
                    "latitude",
                    "longitude",
                    "sog",
                    "navigational_status",
                    "name",]
    available_keep = [c for c in keep_columns if c in parsed.columns]
    cleaned = parsed.select(*available_keep).dropna(subset=["timestamp", "mmsi", "latitude", "longitude"])

    # Use a bounding box filter to limit the data to the area around the center coordinate.
    lat_delta_deg = RADIUS_NM / 60.0
    lon_delta_deg = RADIUS_NM / (60.0 * math.cos(math.radians(CENTER_LAT)))

    box = cleaned.filter((F.col("latitude") >= F.lit(CENTER_LAT - lat_delta_deg))
                          & (F.col("latitude") <= F.lit(CENTER_LAT + lat_delta_deg))
                          & (F.col("longitude") >= F.lit(CENTER_LON - lon_delta_deg))
                          & (F.col("longitude") <= F.lit(CENTER_LON + lon_delta_deg)))

    cleaned = cleaned.filter((F.col("latitude").between(-90, 90)) &
                             (F.col("longitude").between(-180, 180)))

    # Another filter using haversine distance to the center coordinate.
    filtered = box.withColumn("distance_from_center_nm",
                              haversine_nm_expr("latitude", "longitude", CENTER_LAT, CENTER_LON),
                              ).filter(F.col("distance_from_center_nm") <= F.lit(RADIUS_NM))
    
    # Print the number of rows before and after filtering for the file.
    before_count = cleaned.count()
    after_count = filtered.count()

    print(f"{csv_file.name}: "f"{before_count:,} -> {after_count:,}")

    output_file = output_dir / f"{csv_file.stem}.parquet"
    (filtered.write.mode("overwrite")
     .option("compression", "snappy")
     .csv(str(output_file)))


def main() -> None:
    """Main function to run the preprocessing pipeline."""
    # Define Spark session for working with big data.
    spark = (SparkSession.builder.appName("AIS December 2021 Preprocessing")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    # Define input and output directories.
    input_dir = INPUT_DIR
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each daily CSV file and save the cleaned data as parquet files.
    csv_files = sorted(input_dir.glob("aisdk-2021-12-01.csv")) # "aisdk-2021-12-*.csv" for all December files.
    # IF no files are found, raise error .
    if not csv_files:
        raise FileNotFoundError(f"Needed CSV files not found in {input_dir}")
    for csv_file in csv_files:
        preprocess_daily_file(spark, csv_file, output_dir)

    print("Finished preprocessing. Parquet files are saved in Data/Parquet.")
    spark.stop()


if __name__ == "__main__":
    main()
    
