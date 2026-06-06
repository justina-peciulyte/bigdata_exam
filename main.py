import argparse
from pathlib import Path

from collision_analysis import run_collision_analysis
from visualization import save_collision_map
from cli import resolve_path_arg


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Run AIS collision analysis and mapping.")
	parser.add_argument("--parquet-dir", dest="parquet_dir", type=Path, help="Directory containing preprocessed parquet files.")
	parser.add_argument("--output-dir", dest="output_dir", type=Path, help="Directory where the HTML map will be written.")
	parser.add_argument("--map-name", dest="map_name", default=None, help="Filename for the generated HTML map.")
	return parser


def main() -> None:
	args = build_parser().parse_args()
	base_dir = Path(__file__).resolve().parent
	parquet_dir = resolve_path_arg(args.parquet_dir, "AIS_PARQUET_DIR", base_dir / "Data" / "Parquet", base_dir)
	output_dir = resolve_path_arg(args.output_dir, "AIS_OUTPUT_DIR", base_dir / "Output", base_dir)
	map_name = args.map_name if args.map_name else None
	output_dir.mkdir(parents=True, exist_ok=True)

	event, trajectory_pdf, err = run_collision_analysis(parquet_dir)

	if err:
		print(err)
		return

	print("=== Collision/Proximity Detection Result ===")
	print(f"MMSI 1: {event['mmsi_1']}")
	print(f"Vessel 1: {event['name_1']}")
	print(f"MMSI 2: {event['mmsi_2']}")
	print(f"Vessel 2: {event['name_2']}")
	print(f"Event Timestamp: {event['event_timestamp']}")
	print(f"Event Coordinates: ({event['event_latitude']:.6f}, {event['event_longitude']:.6f})")
	print(f"Distance Between Vessels: {event['pair_distance_nm']:.4f} nm")

	final_map_name = map_name if map_name else "collision_trajectory_map.html"
	map_path = save_collision_map(event, trajectory_pdf, output_dir / final_map_name)
	print(f"Trajectory map saved to: {map_path}")


if __name__ == "__main__":
	main()