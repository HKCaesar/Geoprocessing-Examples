Geoprocessing Examples
======================

The included geoprocessing examples are not complete, rigorously tested, or to
be trusted in a production environment and are only meant to be examples.


Setup
-----

```console
$ git clone https://github.com/geowurster/Geoprocessing-Examples.git
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```


Building Extraction
-------------------

Extract building footprints from a LiDAR derived DTM, DEM and 3 band NAIP.  See
the first cell in the notebook for more information.

[iPython Notebook](http://nbviewer.ipython.org/github/geowurster/Geoprocessing-Examples/blob/master/BuildingExtraction.ipynb)


Streaming Topology Operations
-----------------------------

Perform Shapely topology operations on GeoJSON features or geometries.

### Description ###

    Perform Shapely topology operations on GeoJSON features or geometries.

    Topology operations are methods or properties available to
    `shapely.geometry.base.BaseGeometry` that returns a geometry and does
    not require a geometry as an argument.

    By no means perfect or complete but multiple operations can be chained
    together with the `-to` flag.  The result from each operation is passed
    on to the next in the chain so you can do stuff like compute the centroid
    for every feature and then immediately buffer it and write that geometry
    to the output file.

### Examples ###


    Buffer geometries 15 meters:

    $ fio cat sample-data/polygon-samples.geojson \
        | ./streaming-topology-operations.py \
            -to buffer:distance=15

    Compute geometry centroid and buffer that by 100 meters:

    $ fio cat sample-data/polygon-samples.geojson \
        | ./streaming-topology-operations.py \
            -to centroid -to buffer:distance=100

    Compute geometry centroid, buffer 20 meters, and compute envelope:
    
    $ fio cat sample-data/polygon-samples.geojson \
        | ./streaming-topology-operations.py \
            -to centroid -to buffer:distance=20 -to envelope

    Read, transform, write:
    
    $ fio cat sample-data/tl_2014_54037_roads.geojson \
        | ./streaming-topology-operations.py -to buffer:distance=3 \
        | fio load sample-data/buffered/tl_2014_54037_roads.geojson \
            -f GeoJSON --sequence --src_crs EPSG:32618 --dst_crs EPSG:32618


Zonal Statistics
----------------

Get raster stats for every feature in a vector datasource.

### Examples ###

Only compute against the first two bands:

    $ ./zonal-statistics.py sample-data/NAIP.tif \
            sample-data/polygon-samples.geojson -b 1,2

Compute against all bands and also perform a check to see if a geometry is
completely contained within the raster bbox:

    $ ./zonal-statistics.py sample-data/NAIP.tif \
            sample-data/polygon-samples.geojson --contained


### Description ###

Compute zonal statistics for each input feature across all bands of an input
raster.  God help ye who supply large non-block encoded rasters or large
polygons...

By default min, max, mean, standard deviation and sum are computed but the
user can also create their own functions to compute custom metrics across
the intersecting area for every feature and every band.  Functions must
accept a 2D masked array extracted from a single band.  Should probably
be changed to allow the user to compute statistics across all bands.

Use `custom={'my_metric': my_metric_func}` to call `my_metric_func` on the
intersecting pixels.  A key named `my_metric` will be added alongside `min`,
`max`, etc.  Metrics can also be disabled by doing `custom={'min': None}`
to turn off the call to `min`.  The `min` key will still be included in the
output but will have a value of `None`.

While this function will work with any geometry type the input is intended
to be polygons.  The goal of this function is to be able to take large
rasters and a large number of not too giant polygons and be pretty confident
that nothing is going to break.  There are better methods for collecting
statistics if the goal is speed or by optimizing for each datatype. Point
layers will work but are not as efficient and should be used with rasterio's
`sample()` method, and an initial pass to index points against the raster's
blocks.

Further optimization could be performed to limit raster I/O for really
really large numbers of overlapping polygons but that is outside the
intended scope.

In order to handle raster larger than available memory and vector datasets
containing a large number of features, the minimum bounding box for each
feature's geometry is computed and all intersecting raster windows are read.
The inverse of the geometry is burned into this subset's mask yielding only
the values that intersect the feature.  Metrics are then computed against
this masked array.


Summation Raster
----------------

Creation a geometry density map or sum a property.

When summing a property every pixel that intersects a geometry has the value
of the specified property added to the pixel.  So, given two partially
overlapping polygons A and B where A has a value of 1 and B has a value of 2,
pixels in the overlapping area will have a value of 3, pixels in polygon A will
have a value of 1, and pixels in polygon B will have a value of 2.  See below:

Sum a property:

        A = 1
        B = 2
                B
                +------------+
        A       |222222222222|
        +-------+---+22222222|
        |1111111|333|22222222|
        |1111111|333|22222222|
        |1111111+---+--------+
        +-----------+

Compute density:

                B
                +------------+
        A       |111111111111|
        +-------+---+11111111|
        |1111111|222|11111111|
        |1111111|222|11111111|
        |1111111+---+--------+
        +-----------+

Examples:

    Create a point density raster at a 10 meter resolution:

        $ summation-raster.py sample-data/point-sample.geojson OUT.tif \
            --creation-option TILED=YES \
            --target-resolution 10

    Sum a proeprty at a 100 meter resolution

        $ summation-raster.py sample-data/point-sample.geojson OUT.tif \
            --creation-option TILED=YES \
            --target-resolution 100 \
            --property ID

NOTE: Point layers work well but other types are raising the error below but
      all geometry types should work in theory.

    Assertion failed: (0), function query, file AbstractSTRtree.cpp, line 285.
    Abort trap: 6


Delimited Dataset to Vector Datasource
-------------------------------------

### Examples ###

Convert delimited vector data to an OGR datasource.

Print GeoJSON to stdout:

    $ ./delimited2datasource.py sample-data/WV.csv - -gf wkt:WKT

Convert to a shapefile:

    $ ./delimited2datasource.py sample-data/WV.csv WV.shp -gf wkt:WKT

Skip 5 lines and only process the next 10:

    $ ./delimited2datasource.py sample-data/WV.csv - -gf wkt:WKT -sl 5 -ss 10

Reproject geometries:

    $ ./delimited2datasource.py sample-data/WV.csv - -gf wkt:WKT \
        --src_crs EPSG:4269 --dst_crs EPSG:4326

Only write two fields but convert one to an int:

    $ ./delimited2datasource.py sample-data/WV.csv - \
        -gf wkt:WKT -p NAME=str -p STATEFP=int

Read point data from an x and y columns while casting one field to int and one to float:

    $ ./delimited2datasource.py sample-data/WV.csv - \
        -gf xy:centroid_x,centroid_y -p COUNTYFP=int -p ALAND=float

### Description ###

A generator to read a CSV containing vector data and convert to GeoJSON on
the fly.  Geometry can be supplied as WKT, GeoJSON, or points with X and
Y stored in different fields.

The user supplies an iterable, like an instance of `csv.DictReader()`, that
provides one dictionary per iteration that is converted on the fly to a
GeoJSON object and yielded to the parent process.  The user can specify
the geometry field and what kind of geometry representation it contains. 

All keys except `None` or the geometry field are included in the output
features but the user can take control over what fields are included and
what type they are cast to via the `properties` parameter.  If set to `None`
all fields are included in the output feature but if set to a dictionary
where keys are fieldnames and values are functions then only the fields
specified in the keys are included in the output.  The functions must
take a single argument, return a single value, and be prepared to handle
values contained in the associated field in the dictionaries yielded by
the `dict_reader`.

Helper functions have been included to make creating a `properties`
definition containing `str`, `float`, or `int` easier.  They are prefixed
with `helper_` and `helper_properties_def` takes a set of Fiona properties
and returns a definition constructed with the helpers that is suitable for
the `properties` parameter.  The primary job of these helper functions is to
return `None` when they encounter an empty string or `None`.  If the standard
Python `int, float, etc.` functions were used then exceptions would constantly
be raised.