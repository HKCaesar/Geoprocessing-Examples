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


Streaming Topology Operations
-----------------------------

### Examples ###

Perform topology operations on GeoJSON features or geometries.

By no means perfect or complete but multiple operations can be chained
together with the `-to` flag.  The result from each operation is passed
on to the next in the chain so you can do stuff like compute the centroid
for every feature and then immediately buffer it and write that geometry
to the output file.

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


Zonal Statistics
----------------

### Examples ###

Get raster stats for every feature in a vector datasource.

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

Input layers are intended to be polygons in and the goal is a routine that can
safely take a large raster and large number of polygons that do not span too
may raster blocks.  Point layers will work but are not as efficient and
should be used with rasterio's `sample()` method, a raster block iterator,
and a vector layer bbox filter constructed from every block window. The output
metrics would only contain an entry for points that actually intersect the
raster but that's easy enough to handle.

Further optimization could be performed to limit raster I/O for really
really large numbers of overlapping polygons but that is outside the
intended scope.  With a little bit of work the Fiona datasource dependency
could be eliminated by an arbitrary vector iterator/generator and accompanying
CRS definition.

By default min, max, mean, standard deviation and sum are computed but the
user can also create their own functions to compute custom metrics across
the intersecting area for every feature and every band.  Functions must
accept a 2D masked array (extracted from a single band) and return something.
Use `custom={'my_metric': my_metric_func}` to call `my_metric_fun` on the
intersecting pixels.  A key named `my_metric` will be added alongside `min`,
`max`, etc.  Metrics can also be disabled by doing `custom={'min': None}`
to turn off the call to `min`.  The `min` key will still be included in the
output but will have a value of `None`.

In order to handle raster larger than available memory and vector datasets
containing a large number of features, the minimum bounding box for each
feature's geometry is computed and all intersecting raster windows are read.
The inverse of the geometry is burned into this subset's mask yielding only
the values that intersect the feature.  Metrics are then computed against
this masked array.

Example output:

    The outer keys are feature ID's

    {
        '0': {
            'bands': {
                1: {
                    'max': 244,
                    'mean': 97.771298771710065,
                    'min': 15,
                    'std': 44.252917708519028,
                    'sum': 15689067
                }
            },
            'contained': True,
            'outside': False
        },
        '1': {
            'bands': {
                1: {
                    'max': 240,
                    'mean': 102.17252754327959,
                    'min': 14,
                    'std': 43.650764099201055,
                    'sum': 26977532
                }
            },
            'contained': True,
            'outside': False
        }
    }

### Notes ###

Features where the `outside` key is `False` will not have any additional keys.
The `contained` key will only appear in the output if `contained=True`.

### Parameters ###

vector : <fiona feature collection>

    Vector datasource.
    
raster : <rasterio RasterReader>

    Raster datasource.
    
window_band : int, optional

    Specify which band should supply the read windows are extracted from.
    Ideally the windows are identical across all bands.
    
custom : dict or None,

    Supply custom functions as `{'name': func}`.
    
bands : int or list or None, optional

    Bands to compute stats against.  Default is all.
    
contained : bool, optional

    Specify if geometries should be tested to see if they fall completely
    within the raster.  Can be a very expensive computation for geometries
    with a large number of vertexes.  Setting to `False` will yield a
    performance boost for these situations.

### Returns ###

dict
    See 'Example output' above.


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

Generator to read a CSV containing vector data and convert to GeoJSON on
the fly.  Geometry can be supplied as WKT, GeoJSON, or points with X and
Y stored in different fields.

The user supplies an iterable, like an instance of `csv.DictReader()`, that
provides one dictionary per iteration that is converted on the fly to a
GeoJSON object and yielded to the parent process.  The user can specify
which field contains the geometry and what kind of geometry representation
it contains.

All keys except `None` or the geometry field are included in the output
features but the user can specify what fields to include and what type
they should be cast to via the `properties` parameter.  If set to `None`
all fields are included in the output feature but if set to a dictionary
where keys are fieldnames and values are functions then only the fields
specified in the keys are included in the output.  The functions must
take a single argument, return a single value, and be prepared to handle
values contained in the associated field in the dictionaries yielded by
the `dict_reader`.

Helper functions have been included to make creating a `properties`
definition containing `str`, `float`, or `int` easier.  They are prefixed
with `helper_` and `helper_properties_def` takes a set of Fiona properties
and returns a definition suitable for the `properties` parameter that uses
the helper functions.

Example `properties` definition:

    {
        'field1': float,
        'field2': helper_int,
        'field3': custom_function
    }

Create a `properties` definition from a set of Fiona properties:

Valid `geomtype_field` definitions:

    wkt:field
    geojson:field
    xy:x_field,y_field[,z_field]

### Parameters ###

dict_reader : csv.DictReader (or other iterable object returning dicts)

    A loaded instance configured with whatever settings the user desires.
    
skip_failures : bool, optional

    If `True` and an exception is encountered while iterating over rows, that
    row will be skipped and the exception will be logged.  If `False` the
    exception is raised.
    
geomtype_field : str

    Geometry type and field description.
    
properties : dict, optional

    Keys are fieldnames in the CSV and vals are functions used to cast the
    values to a Python type.  If `None` then all fields are converted to
    strings.  Fields that are `None` or are specified in the
    `geomtype_field` argument are ignored even if they are specified in the
    `properties` parameter.

### Yields ###

dict
    A GeoJSON feature.