Geoprocessing Examples
======================


Setup
-----

```console
$ git clone https://github.com/geowurster/Geoprocessing-Examples.git
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```


Streaming Topology Operations
-----------------------------

Perform topology operations on GeoJSON features or geometries.

By no means perfect or complete but multiple operations can be chained
together with the `-to` flag.

Examples:

    Buffer geometries 15 meters:
    $ fio cat sample-data/polygon-samples.geojson | \
        ./streaming-topology-operations.py \
        -to buffer:distance=15

    Compute geometry centroid and buffer that by 100 meters:
    $ fio cat sample-data/polygon-samples.geojson | \
        ./streaming-topology-operations.py \
        -to centroid -to buffer:distance=100

    Compute geometry centroid, buffer 20 meters, and compute envelope:
    $ fio cat sample-data/polygon-samples.geojson | \
        ./streaming-topology-operations.py \
        -to centroid -to buffer:distance=20 -to envelope

To dump to a file pipe to `fio load`.



