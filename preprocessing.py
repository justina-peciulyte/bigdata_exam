import argparse
import math
import os
import re
from pathlib import Path
from cli import resolve_path_arg, env_float

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Input and output paths.
INPUT_DIR = Path("Data/Extracted")
OUTPUT_DIR = Path("Data/Parquet")

# Geografical bounds for filtering vessels:
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM = 50.0


# def _env_path(name: str, default: Path) -> Path:
#     value = os.environ.get(name)
#     return Path(value) if value else default


# def _env_float(name: str, default: float) -> float:
#     value = os.environ.get(name)
#     return float(value) if value is not None and value != "" else default


def build_parser() -> argparse.ArgumentParser:
    """Builds the CLI argument parser specifically for the preprocessing script."""
    parser = argparse.ArgumentParser(description="Preprocess AIS CSV files into parquet.")
    parser.add_argument("--input-dir", dest="input_dir", type=Path, help="Input directory containing daily CSV files.")
    parser.add_argument("--input-glob", dest="input_glob", default=os.environ.get("AIS_INPUT_GLOB", "aisdk-2021-12-*.csv"), help="File (glob) pattern for daily CSV files.")
    parser.add_argument("--output-dir", dest="output_dir", type=Path, help="Output directory for parquet files.")
    parser.add_argument("--center-lat", dest="center_lat", type=float, help="Center latitude for spatial filtering.")
    parser.add_argument("--center-lon", dest="center_lon", type=float, help="Center longitude for spatial filtering.")
    parser.add_argument("--radius-nm", dest="radius_nm", type=float, help="Radius in NM for spatial filtering.")
    return parser


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


def preprocess_daily_file(spark: SparkSession,
                          csv_file: Path,
                          output_dir: Path,
                          center_lat: float,
                          center_lon: float,
                          radius_nm: float) -> None:
    """Preprocesses a single daily CSV file and saves it to a parquet file."""
    print(f"Processing file: {csv_file.name}")

    # 1. With Spark, read the CSV file.
    raw_df = (spark.read.option("header", True)
              .option("mode", "PERMISSIVE")
              .csv(str(csv_file)))

    # 2. Normalize all column names at once.
    normalized_cols = [normalize_col_name(c) for c in raw_df.columns]
    df = raw_df.toDF(*normalized_cols)

    # 3. Check for required columns in collision analysis and parse data types.
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

    # 4. Keep only fields needed by later collision analysis and drop others for efficiency.
    keep_columns = ["timestamp",
                    "mmsi",
                    "latitude",
                    "longitude",
                    "sog",
                    "navigational_status",
                    "name",]
    cleaned = parsed.select(*keep_columns).dropna(subset=["timestamp", "mmsi", "latitude", "longitude"])

    # 5. Use a bounding box filter to limit the data to the area around the center coordinate.
    lat_delta_deg = radius_nm / 60.0
    lon_delta_deg = radius_nm / (60.0 * math.cos(math.radians(center_lat)))

    cleaned = cleaned.filter(F.col("latitude").between(-90, 90) & 
                             F.col("longitude").between(-180, 180))

    box = cleaned.filter((F.col("latitude") >= F.lit(center_lat - lat_delta_deg))
                         & (F.col("latitude") <= F.lit(center_lat + lat_delta_deg))
                         & (F.col("longitude") >= F.lit(center_lon - lon_delta_deg))
                         & (F.col("longitude") <= F.lit(center_lon + lon_delta_deg)))

    # 6. Another filter using haversine distance to the center coordinate.
    filtered = box.withColumn("distance_from_center_nm",
                              haversine_nm_expr("latitude", "longitude", center_lat, center_lon),
                              ).filter(F.col("distance_from_center_nm") <= F.lit(radius_nm))
    
    # 7. Print the number of rows before and after filtering for the file.
    before_count = cleaned.count()
    filtered = filtered.cache()
    after_count = filtered.count()

    print(f"{csv_file.name}: "f"{before_count:,} -> {after_count:,}")

    # 8. Save the resulting dataframe as a parquet file in the output directory.
    output_file = output_dir / f"{csv_file.stem}.parquet"
    (filtered.write.mode("overwrite")
     .option("compression", "snappy")
     .parquet(str(output_file)))


def main() -> None:
    """Main function to run the preprocessing pipeline."""
    args = build_parser().parse_args()
    base_dir = Path(__file__).resolve().parent
    input_dir = resolve_path_arg(args.input_dir, "AIS_INPUT_DIR", base_dir / "Data" / "Extracted", base_dir)
    output_dir = resolve_path_arg(args.output_dir, "AIS_PARQUET_DIR", base_dir / "Data" / "Parquet", base_dir)
    center_lat = args.center_lat if args.center_lat is not None else env_float("AIS_CENTER_LAT", CENTER_LAT)
    center_lon = args.center_lon if args.center_lon is not None else env_float("AIS_CENTER_LON", CENTER_LON)
    radius_nm = args.radius_nm if args.radius_nm is not None else env_float("AIS_RADIUS_NM", RADIUS_NM)
    input_glob = args.input_glob

    # 1. Define Spark session for working with big data.
    spark = (SparkSession.builder.appName("AIS December 2021 Preprocessing")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    # 2. Define input and output directories.
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3. Process each daily CSV file and save the cleaned data as parquet files.
    csv_files = sorted(input_dir.glob(input_glob))
    # If no files are found, raise an error.
    if not csv_files:
        raise FileNotFoundError(f"Needed CSV files not found in {input_dir}")
    for csv_file in csv_files:
        preprocess_daily_file(spark, csv_file, output_dir, center_lat, center_lon, radius_nm)

    # 4. After preprocessing, stop the Spark session.
    print(f"Finished preprocessing. Parquet files are saved in {output_dir}.")
    spark.stop()


if __name__ == "__main__":
    main()
