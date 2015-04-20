Geoprocessing Examples
======================

The code in this is only meant to be an example is not rigorously tested,
complete, or to be trusted in a production environment.
 
Sample data sources:

* LAZ - [USGS Earth Explorer](http://earthexplorer.usgs.gov)
* Roads, hydrography, and political boundaries - [U.S. Census Bureau](https://www.census.gov/geo/maps-data/data/tiger-line.html)
* NAIP - [NRCS Data Gateway](http://datagateway.nrcs.usda.gov)


Setup
-----

```console
$ git clone https://github.com/geowurster/Geoprocessing-Examples.git
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```


Running an Example
------------------

Each utility/script in this repo is executable and has a builtin help page with
example commands.  For instance:
 
```console
$ ./zonal-statistics.py --help
Usage: zonal-statistics.py [OPTIONS] RASTER VECTOR

  Get raster stats for every feature in a vector datasource.

  Only compute against the first two bands:
  
      $ zonal-statistics.py sample-data/NAIP.tif \
          sample-data/polygon-samples.geojson -b 1,2
  
  Compute against all bands and also perform a check to see if a geometry is
  completely contained within the raster bbox:
  
      $ zonal-statistics.py sample-data/NAIP.tif \
          sample-data/polygon-samples.geojson --contained

Options:
  -b, --bands TEXT         Bands to process as `1' or `1,2,3'.
  -at, --all-touched       Enable 'all-touched' rasterization.
  -c, --contained          Enable 'contained' test.
  -npp, --no-pretty-print  Print stats JSON on one line.
  --indent INTEGER         Pretty print indent.
  --help                   Show this message and exit.

```

Documentation
-------------

Each example is well documented with both inline comments and function/class
docstrings with expected parameters, output, algorithmic details, and implementation
details.


Requirements
------------

Instead of using the built-in [Python bindings for GDAL](http://gdal.org/python/) the examples rely on
the more modern [Shapely](http://toblerity.org/shapely/) for geometry operations, [Fiona](http://toblerity.org/fiona/) for vector I/O,
and [rasterio](https://github.com/mapbox/rasterio) for raster I/O.  The remaining requirements are primarily from the [SciPy stack](http://www.scipy.org/stackspec.html)
and its extended family.
