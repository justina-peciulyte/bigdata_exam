# Big Data Examination: Detection of Proximity Events

Author: Justina Pečiulytė, <justina.peciulyte@mif.stud.vu.lt>.

## Overview

The objective of this assignment is to analyze large AIS datasets using Apache Spark in order to identify vessel pairs that experienced the closest physical proximity within a specified marine area. The project uses Spark for efficient big data processing and Parquet file format for faster retrieval and storage. The final output is presented as an HTML map showing +-10 trajectories for involved vessels. The implementation did not verify any physical collisions as discussed in the Limitations section.

The assignment was completed in two main parts – dataset preparation and closest vessel encounter analysis. In the data preparation phase, daily CSV files were first extracted from the large AIS archive and preprocessed to obtain a standardized and reduced dataset, before minimizing it further by converting into Parquet file format. Preprocessing steps include:

- **Normalizing all columns** – converting text to lowercase, separating words with `_`, removing any whitespace or special symbols (`#`).
- **Dropping unneeded columns** – to save storage space, only columns required in the collision analysis were kept.
- **Filtering with bounding box** – filtering points that are within a lat/lon delta that corresponds to the radius in NM.
- **Filtering based on Haversine distance** – filtering observations from the previous step using a 50 NM radius around the center coordinate.

The preprocessing pipeline reduced the dataset size from 52.8 GB to approximately 712 MB (98.6% reduction), making it much more managable to parse and analyze. 

The image is available on the Docker Hub image registry: <https://hub.docker.com/repository/docker/justinap4/collision_analysis>. The Python scripts, Dockerfile, and requirements.txt are available on GitHub: <https://github.com/justina-peciulyte/bigdata_exam/>.

## Data Description

The dataset used is provided by the Danish Maritime Authority: <http://aisdata.ais.dk/>; and covers the whole month of December in 2021. Additionally, we are interested in vessels operating within a 50 nautical mile radius area surrounding the center coordinate located at latitude: 55.225000, longitude: 14.245000. 

The project is built around key variables which must be present in the dataset when running the project:

- MMSI
- Timestamp
- Longitude
- Latitude
- Name
- Navigational status
- Ship type

***Important note:*** for the code to read the files correctly, the CSV files must follow the naming structure of `aisdk-2021-12-*.csv`.

## Design Choices 

The raw AIS dataset contains a variety of data quality issues that may lead to expensive computations and inaccurate results if left untreated. Therefore, several filtering and preprocessing steps mentioned previously were introduced before proximity analysis. Their use is discussed in this section.

**Stationary Vessel Removal**

Many vessels remain largely stationary for long periods of time. Such vessels may appear close to other nearby vessels in ports and harbors, creating false proximity events. To reduce this effect, vessels were filtered using two criteria:

- Median Speed Over Ground (SOG) greater than 0.5 knots – mostly stationary vessels are removed and possible GPS jumps of otherwise normal vessel tracks are avoided entirely. 
- Navigational status not indicating anchored, moored, or aground conditions – this filters out explicitely non-moving vessels.

**GPS Anomaly Detection**

AIS observations occasionally contain pings that imply physically impossible vessel movements. Such observations were identified by calculating the implied speed between consecutive observations of the same vessel. Rows producing an implied speed greater than 40 knots (significantly above the threshold for most vessels) were considered GPS anomalies and removed from further analysis.

**Efficient Proximity Detection**

Pairwise comparisons between every vessel would result in an impractical Cartesian product. Instead, to improve efficiency, observations were first grouped into:

- Time buckets (60-second intervals).
- Spatial grid cells based on latitude and longitude.

Only observations sharing the same temporal and spatial bucket were compared. Next, exact geographical distances were then calculated using the Haversine distance formula and filtered using a  proximity threshold. This significantly reduced the number of distance calculations while preserving nearby vessel encounters.

## Limitations

While the proposed code solution succesfully processes large-scale data and identifies vessel pairs that experienced extremely close spatial proximity, several limitations to the approach must be acknowledged. Most importantly, the distinction between close proximity encounters and actual physical collision between vessels is not realized. 

Many detected proximity events involved rescue vessels and other harbor service vessels operating in close proximity. Such vessels may remain within a few meters of each other for extended periods of time appearing as loitering and causing them to rank highly according to criteria based on distance or number of proximity events despite not representing collisions. A portion of these cases was remedied by excluding a few specific ship types (tug boats, law enforcement, etc.) but some additional categories may have been missed or simply not possible to account for due to incomplete data. 

Therefore, the current implementation identifies the most significant proximity events rather than guaranteeing the detection of a verified physical collision. Additional information such as vessel heading or a more rigorous approach to vessel motion analysis would likely improve collision identification accuracy. Future improvements could include analysis on gaps between observations to verify collisions, as severe crashes between vessels may affect the GPS reporting systems and result in an AIS blackout for one or even both ships involved.

## Detected Closest Proximity Event

The algorithm detected several vessel pairs exhibiting sustained close proximity. The highest-ranked encounter involved:

- MMSI 1: 261002520 (name: DZI-100)
- MMSI 2: 261018880 (name: DZI-10)
- Minimum distance: 0.0103 NM
- Event timestamp: 2021-12-17 20:50:13
- Event coordinates: (54.634779, 14.345543)

Interestingly, both involved vessels fishing ships under the Polish flag. Although close proximity (19.07 m) was detected between the two ships, the extracted trajectories surrounding the event show a likely normal encounter. The vessels move close together throughout the 20 minute window and cross each other's trajectories. Trajectory visualization for the selected encounter was generated and exported as an interactive HTML map available in the repository.

## Project Structure

The project is structured as follows:

- `main.py` – entrypoint of the project. Calls for `collision_analysis.py` and `visualization.py` modules to run the whole pipeline.
- `preprocessing.py` – loads original CSV data and performs preprocessing. This step is separated from the main pipeline to avoid repeatedly reading the large CSV datasets when running the project. 
- `collision_analysis.py` – analyses proximity events. The module loads Parquet files, filters out stationary vessels (both by SOG and navigational status), removes rows with GPS anomalies, detects proximity events, and extracts travel trajectories of involved vessels.
- `visualization` – visualizes proximity/collision events and vessel trajectories.
- `cli.py` – contains helper functions and environment variables which are used by other modules.

While the Docker image creation relies on:

- `Dockerfile` – defines the container image.
- `requirements.txt` – includes a list of Python dependencies.

## Run Instructions

**Local Solution**

1. Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Preprocess CSV files into Parquet:

```bash
python preprocessing.py --input-dir Data/Extracted --output-dir Data/Parquet
```

3. Run collision/proximity analysis and create HTML map by specifying input data path and output folder:

```bash
python main.py --parquet-dir Path/to/Parquet --output-dir Path/to/Output
```

---

**Docker Solution**

The container is built using the following command:

```bash
docker build -t ais-collision .
```

```bash
docker run --rm -v ${PWD}:/workspace ais-collision
```
The container runs `main.py` by default. To run preprocessing instead, pass a different script:

```bash
docker run --rm -v ${PWD}:/workspace ais-collision python preprocessing.py --input-dir Data/Extracted --output-dir Data/Parquet
```

## Configurable Parameters

Configurable parameters can be checked by overriding the CMD and running one of the Python files:

1. For `main.py`:

```
docker run --rm -v $(pwd):/workspace justinap4/ais-collision:latest python main.py --help
```

| Parameter     | Description                                                         |
|---------------|---------------------------------------------------------------------|
| --parquet-dir | Directory containing preprocessed parquet files (*must be mounted into the container*).   |
| --output-dir  | Directory where the HTML map will be written (creates `Output` folder by default).                                  |
| --map-name    | Filename for the generated HTML map.    |

2. For `preprocessing.py`:

```
docker run --rm -v $(pwd):/workspace justinap4/ais-collision:latest python preprocessing.py --help
```

| Parameter     | Description                                                         |
|---------------|---------------------------------------------------------------------|
| --parquet-dir | Directory containing preprocessed parquet files (*must be mounted into the container*).   |
| --output-dir  | Directory where the HTML map will be written (creates `Output` folder by default).                                  |
| --map-name    | Filename for the generated HTML map.    |

## AI Usage Disclosure

In this project, artificial intelligence tools were used for:

- Interpreting and suggesting fixes for errors in written code, Spark installation and use. 
- Creating a unified module for resolving CLI argument paths. This was done to have a single source of helper functions and simplify local parsers.
- Implementing time bucket and spatial grids for comparing vessel pairs.
- General error fixes and improvements for written code.
