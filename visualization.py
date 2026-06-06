from pathlib import Path
import folium


def _build_track_points(trajectory_pdf, target_mmsi: int):
	subset = trajectory_pdf[trajectory_pdf["mmsi"] == target_mmsi].sort_values("timestamp")
	return list(zip(subset["latitude"].tolist(), subset["longitude"].tolist()))


def build_collision_map(event: dict, trajectory_pdf):
	"""Create a map for +-10 minute vessel trajectories around event time."""
	center = [event["event_latitude"], event["event_longitude"]]
	m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

	mmsi_1 = event["mmsi_1"]
	mmsi_2 = event["mmsi_2"]

	pts_1 = _build_track_points(trajectory_pdf, mmsi_1)
	pts_2 = _build_track_points(trajectory_pdf, mmsi_2)

	if pts_1:
		folium.PolyLine(pts_1,
				  color="#c51eba",
				  weight=4,
			      opacity=0.85,
			      tooltip=f"{event['name_1']} ({mmsi_1})",).add_to(m)
		folium.CircleMarker(pts_1[0], radius=5, color="#d73027", fill=True).add_to(m)

	if pts_2:
		folium.PolyLine(
			pts_2,
			color="#4575b4",
			weight=4,
			opacity=0.85,
			tooltip=f"{event['name_2']} ({mmsi_2})",).add_to(m)
		folium.CircleMarker(pts_2[0], radius=5, color="#4575b4", fill=True).add_to(m)

	popup = (
		f"Collision/proximity time: {event['event_timestamp']}<br>"
		f"Vessel 1: {event['name_1']} ({mmsi_1})<br>"
		f"Vessel 2: {event['name_2']} ({mmsi_2})<br>"
		f"Distance: {event['pair_distance_nm']:.4f} nm"
	)
	folium.Marker(location=center,
			   	  popup=popup,
				  tooltip="Detected collision/proximity point",
				  icon=folium.Icon(color="red", icon="info-sign"),).add_to(m)
	return m


def save_collision_map(event: dict, trajectory_pdf, output_html: Path) -> Path:
	output_html.parent.mkdir(parents=True, exist_ok=True)
	m = build_collision_map(event, trajectory_pdf)
	m.save(str(output_html))
	return output_html
