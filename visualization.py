from pathlib import Path
import folium


# Visualization functions for collision/proximity events and trajectories.


def _build_track_points(trajectory_pdf, target_mmsi: int):
	"""Extract a list of coordinates for a given MMSI from the trajectory pandas dataframe."""
	subset = trajectory_pdf[trajectory_pdf["mmsi"] == target_mmsi].sort_values("timestamp")
	return list(zip(subset["latitude"].tolist(), subset["longitude"].tolist()))


def _add_ping_markers(map_obj, trajectory_pdf, target_mmsi: int, vessel_name: str, color: str):
	"""Function to add point markers for each AIS ping in the trajectory map."""
	subset = trajectory_pdf[trajectory_pdf["mmsi"] == target_mmsi].sort_values("timestamp")
	for _, row in subset.iterrows():
		timestamp_text = str(row["timestamp"])
		# Create a popup with vessel name, timestamp, coordnites, and SOG info.
		popup = (
			f"Vessel: {vessel_name} ({target_mmsi})<br>"
			f"Time: {timestamp_text}<br>"
			f"Lat/Lon: ({row['latitude']:.6f}, {row['longitude']:.6f})<br>"
			f"SOG: {float(row['sog']):.2f} kn"
		)
		# Add a circle marker with folium.
		folium.CircleMarker(
			location=[row["latitude"], row["longitude"]],
			radius=3,
			color=color,
			fill=True,
			fill_opacity=0.9,
			popup=popup,
			tooltip=timestamp_text,
		).add_to(map_obj)


def build_collision_map(event: dict, trajectory_pdf):
	"""Create a map for +-10 minute vessel trajectories around event time."""
	# 1. Start with a base folium map centered on the location of the event.
	center = [event["event_latitude"], event["event_longitude"]]
	m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")
	# 2. Extract the trajectories for both vessels and add them to the map.
	mmsi_1 = event["mmsi_1"]
	mmsi_2 = event["mmsi_2"]

	pts_1 = _build_track_points(trajectory_pdf, mmsi_1)
	pts_2 = _build_track_points(trajectory_pdf, mmsi_2)
	# Define aesthetic styles for the two vessels.
	if pts_1:
		folium.PolyLine(pts_1,
				  color="#c51eba",
				  weight=4,
			      opacity=0.85,
			      tooltip=f"{event['name_1']} ({mmsi_1})",).add_to(m)
		folium.CircleMarker(pts_1[0], radius=5, color="#d73027", fill=True).add_to(m)
		_add_ping_markers(m, trajectory_pdf, mmsi_1, event["name_1"], "#c51eba")

	if pts_2:
		folium.PolyLine(
			pts_2,
			color="#4575b4",
			weight=4,
			opacity=0.85,
			tooltip=f"{event['name_2']} ({mmsi_2})",).add_to(m)
		folium.CircleMarker(pts_2[0], radius=5, color="#4575b4", fill=True).add_to(m)
		_add_ping_markers(m, trajectory_pdf, mmsi_2, event["name_2"], "#4575b4")
	# 3. Add a marker for the collision/proximity event with a popup for details.
	popup = (
		f"Collision/proximity time: {event['event_timestamp']}<br>"
		f"Vessel 1: {event['name_1']} ({mmsi_1})<br>"
		f"Vessel 2: {event['name_2']} ({mmsi_2})<br>"
		f"Distance: {event['pair_distance_nm']:.4f} nm"
	)
	# 4. Change the marker style for the event point and return the map.
	folium.Marker(location=center,
			   	  popup=popup,
				  tooltip="Detected collision/proximity point",
				  icon=folium.Icon(color="red", icon="info-sign"),).add_to(m)
	return m


def save_collision_map(event: dict, trajectory_pdf, output_html: Path) -> Path:
	"""Saves the output to an HTML file."""
	output_html.parent.mkdir(parents=True, exist_ok=True)
	m = build_collision_map(event, trajectory_pdf)
	m.save(str(output_html))
	return output_html
