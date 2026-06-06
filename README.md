# Big Data Examination: Detection of Vessel Collisions

Justina Pečiulytė, <justina.peciulyte@mif.stud.vu.lt>.

## Overview

The objective of this assignment is to analyze large amounts of AIS data to detect vessel collision or closest proximity events of two vessels within a specified marine area. The project uses Spark for efficient big data processing and Parquet file format for faster retrieval and storage. The final output is presented as an HTML map showing +-10 trajectories for involved vessels.

The assignment was completed in two main parts – dataset preparation and collision analysis. In the data preparation phase, daily CSV files were first extracted from the large AIS archive and preprocessed to obtain a standardized and reduced dataset, before minimizing it further by converting into Parquet file format. Preprocessing steps include:

- **Normalizing all columns** – converting text to lowercase, separating words with `_`, removing any whitespace or special symbols (`#`).
- **Dropping unneeded columns** – to save storage space, only columns required in the collision analysis were kept.
- **Filtering with bounding box** – filtering points that are within a lat/lon delta that corresponds to the radius in NM.
- **Filtering based on Haversine distance** – filtering observations from the previous step using a 50 NM radius around the center coordinate.

The dataset was reduced from 52.8 GB to 712 MB, making it much more managable to parse and analyze. 

The image is available on the Docker Hub image registry: <https://github.com/justina-peciulyte/bigdata_exam/tree/main>. The Python scripts, Dockerfile, and requirements.txt are available on GitHub: .

### Data Description

The dataset used is provided by the Danish Maritime Authority: <http://aisdata.ais.dk/>; and covers the whole month of December in 2021. Additionally, we are interested in vessels operating within a 50 nautical mile radius area surrounding the center coordinate located at latitude: 55.225000, longitude: 14.245000. 

The project is built around key variables which must be present in the dataset when running the project:

- MMSI
- Timestamp
- Longitude
- Latitude
- Name

***Important note:*** for the code to read the files correctly, the CSV files must follow the naming structure of `aisdk-2021-12-*.csv`.

## Project Structure

The project is structured as follows:

- `main.py` – entrypoint of the project. Calls for `collision_analysis.py` and `visualization.py` modules to run the whole pipeline.
- `preprocessing.py` – loads original CSV data and performs preprocessing. 
- `collision_analysis.py` – analyses proximity events. The module loads Parquet files, filters out stationary vessels (both by SOG and navigational status), removes rows with GPS anomalies, detects proximity events, and extracts travel trajectories of involved vessels.
- `visualization` – visualizes proximity/collision events and vessel trajectories.
- `cli.py` – contains helper functions and environment variables which are used by other modules.

While the Docker image creation relies on:

- `Dockerfile` – defines the container image.
- `requirements.txt` – includes a list of Python dependencies.

## Detected Event Results

## Instructions to set up the Docker container

## Instructions to run the code

## Configurable Parameters

## Design Choices?

## AI Usage Disclosure

In this project, artificial intelligence tools were used for:

- Interpreting and suggesting fixes for errors in written code, Spark installation and use. 
- Creating a unified module for resolving CLI argument paths. This was done to have a single source of helper functions and simplify local parsers.
- General fixes for mistakes in code.

