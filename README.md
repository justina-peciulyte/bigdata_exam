# Big Data Examination: Detection of Vessel Collisions

Justina Pečiulytė, <justina.peciulyte@mif.stud.vu.lt>.

## Overview

The objective of this assignment is to analyze large amounts of AIS data to detect vessel collision or closest proximity events of two vessels within a specified marine area. The project uses Spark fro efficient big data processing and Parquet file format for faster retrieval and storage. 

The dataset used is provided by the Danish Maritime Authority (original source: <http://aisdata.ais.dk/>) and covers the whole month of December in 2021. Additionally, we are interested in vessels operating within a 50 nautical mile radius area surrounding the center coordinate located at latitude: 55.225000, longitude: 14.245000. The code is built around key variables which must be present in the dataset when running the project:

- MMSI
- Timestamp
- Longitude
- Latitude
- Name

The containerized project is available at: LINK; all code is uploaded to a Github repository at: LINK.

## Detected Event Results

## Instructions to set up the Docker container

## Instructions to run the code

## Configurable Arguments

## Module Description

## Design Choices?

## AI Usage Disclosure

In this project, artificial intelligence tools were used for:

- Interpreting and suggesting fixes for errors in written code, Spark installation and use. 
- Creating a unified module for resolving CLI argument paths. This was done to have a single source of helper functions and simplify local parsers.

