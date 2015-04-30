#!/usr/bin/env python


"""
Compute zonal statistics for every feature in a vector datasource across every
band in a raster datasource.
"""


import pprint
import warnings

import click
import fiona as fio
from fiona.transform import transform_geom
import numpy as np
import rasterio as rio
from rasterio.features import rasterize
from shapely.geometry import asShape


warnings.filterwarnings('ignore')


def cb_bands(ctx, param, value):

    """
    Click callback for parsing and validating `--bands`.

    Parameters
    ----------
    ctx : click.Context
        Ignored.
    param : click.Parameter
        Ignored.
    value : str
        See the decorator for `--bands`.

    Returns
    -------
    tuple
        Band indexes to process.
    """

    if value is None:
        return value
    else:
        return sorted([int(i) for i in value.split(',')])


def zonal_stats_from_raster(vector, raster, bands=None, all_touched=False, custom=None):

    """
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
            }
        }

    Parameters
    ----------
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

    Returns
    -------
    dict
        See 'Example output' above.
    """

    if bands is None:
        bands = list(range(1, raster.count + 1))
    elif isinstance(bands, int):
        bands = [bands]
    else:
        bands = sorted(bands)

    metrics = {
        'min': lambda x: x.min(),
        'max': lambda x: x.max(),
        'mean': lambda x: x.mean(),
        'std': lambda x: x.std(),
        'sum': lambda x: x.sum()
    }

    if custom is not None:
        metrics.update(**custom)

    # Make sure the user gave all callable objects or None
    for name, func in metrics.items():
        if func is not None and not hasattr(func, '__call__'):
            raise click.ClickException(
                "Custom function `%s' is not callable: %s" % (name, func))

    r_x_min, r_y_min, r_x_max, r_y_max = raster.bounds

    feature_stats = {}
    for feature in vector:

        """
        rasterize(
            shapes,
            out_shape=None,
            fill=0,
            out=None,
            output=None,
            transform=Affine(1.0, 0.0, 0.0, 0.0, 1.0, 0.0),
            all_touched=False,
            default_value=1,
            dtype=None
        )
        """

        stats = {'bands': {}}

        reproj_geom = asShape(transform_geom(
            vector.crs, raster.crs, feature['geometry'], antimeridian_cutting=True))
        x_min, y_min, x_max, y_max = reproj_geom.bounds

        if (r_x_min <= x_min <= x_max <= r_x_max) and (r_y_min <= y_min <= y_max <= r_y_max):
            stats['contained'] = True
        else:
            stats['contained'] = False

        col_min, row_max = ~raster.affine * (x_min, y_min)
        col_max, row_min = ~raster.affine * (x_max, y_max)
        window = ((row_min, row_max), (col_min, col_max))

        rasterized = rasterize(
            shapes=[reproj_geom],
            out_shape=(row_max - row_min, col_max - col_min),
            fill=1,
            transform=raster.window_transform(window),
            all_touched=all_touched,
            default_value=0,
            dtype=rio.ubyte
        ).astype(np.bool)

        for bidx in bands:

            stats['bands'][bidx] = {}

            data = raster.read(indexes=bidx, window=window, boundless=True, masked=True)

            # This should be a masked array, but a bug requires us to build our own:
            # https://github.com/mapbox/rasterio/issues/338
            if not isinstance(data, np.ma.MaskedArray):
                data = np.ma.array(data, mask=data == raster.nodata)

            data.mask += rasterized

            for name, func in metrics.items():
                if func is not None:
                    stats['bands'][bidx][name] = func(data)

            feature_stats[feature['id']] = stats

    return feature_stats


@click.command()
@click.argument('raster')
@click.argument('vector')
@click.option(
    '-b', '--bands', callback=cb_bands,
    help="Bands to process as `1` or `1,2,3`."
)
@click.option(
    '-a', '--all-touched', is_flag=True,
    help="Enable 'all-touched' rasterization."
)
@click.option(
    '-n', '--no-pretty-print', is_flag=True,
    help="Print stats JSON on one line."
)
@click.option(
    '--indent', type=click.INT, default=0,
    help="Pretty print indent."
)
def main(raster, vector, bands, all_touched, no_pretty_print, indent):

    """
    Get raster stats for every feature in a vector datasource.

    \b
    Only compute against the first two bands:
    \b
        $ zonal-statistics.py sample-data/NAIP.tif \\
            sample-data/polygon-samples.geojson -b 1,2
    \b
    """

    with fio.drivers(), rio.drivers():
        with rio.open(raster) as src_r, fio.open(vector) as src_v:

            if not bands:
                bands = list(range(1, src_r.count + 1))

            results = zonal_stats_from_raster(
                src_v, src_r, bands=bands, all_touched=all_touched)

            if not no_pretty_print:
                results = pprint.pformat(results, indent=indent)

            click.echo(results)


if __name__ == '__main__':
    main()
