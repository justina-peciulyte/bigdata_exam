FROM jupyter/pyspark-notebook:python-3.11

USER root

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1 \
    AIS_INPUT_DIR=/workspace/Data/Extracted \
    AIS_INPUT_GLOB=aisdk-2021-12-*.csv \
    AIS_PARQUET_DIR=/workspace/Data/Parquet \
    AIS_OUTPUT_DIR=/workspace/Output \
    AIS_MAP_NAME=collision_trajectory_map.html

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /workspace

ENTRYPOINT []
CMD ["python", "main.py"]