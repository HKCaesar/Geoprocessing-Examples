#!/usr/bin/env python


"""
Compute zonal statistics for every feature in a vector datasource across every
band in a raster datasource.

Names are overly verbose for ease of following what does what.
"""


import pprint
import sys

import affine
import click
import fiona
import fiona.transform
import numpy as np
import rasterio
import rasterio.features
import rtree.index
import shapely.geometry


def zonal_stats_from_raster(vector, raster, bands=None, contained=True, all_touched=None, custom=None, window_band=0):

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

    Notes
    -----

    Features where the `outside` key is `False` will not have any additional keys.
    The `contained` key will only appear in the output if `contained=True`.

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
    contained : bool, optional
        Specify if geometries should be tested to see if they fall completely
        within the raster.  Can be a very expensive computation for geometries
        with a large number of vertexes.  Setting to `False` will yield a
        performance boost for these situations.

    Returns
    -------
    dict
        See 'Example output' above.
    """

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
            raise ValueError("Custom function `%s' is not callable: %s" % (name, func))

    # User didn't specify which bands - process all
    if bands is None:
        bands = range(1, raster.count + 1)
    elif isinstance(bands, int):
        bands = [bands]
    else:
        bands = sorted(bands)  # Otherwise the output stats could be in the wrong order

    stats = {}

    # Cache raster and vector CRS and generate a raster bbox geometry
    raster_crs = raster.crs
    vector_crs = vector.crs
    _r_min_x, _r_min_y, _r_max_x, _r_max_y = raster.bounds
    raster_bounds = shapely.geometry.Polygon(
        [[_r_min_x, _r_min_y], [_r_min_x, _r_max_y], [_r_max_x, _r_max_y], [_r_max_x, _r_min_y]]
    )

    # Create a spatial index consisting of the raster block's in the coordinate space
    window_index = rtree.index.Index(interleaved=True)
    window_ids = {}
    for idx, block in enumerate(raster.block_windows(window_band)):
        _row, _col = block[1]
        row_min, row_max = _row
        col_min, col_max = _col
        col_min, row_min = raster.affine * (col_min, row_min)
        col_max, row_max = raster.affine * (col_max, row_max)
        window_index.insert(idx, (col_min, row_max, col_max, row_min))
        window_ids[idx] = block

    for feature in vector:

        # Reproject feature's geometry to the raster's CRS and convert to a shapely geometry
        zone_geometry = shapely.geometry.asShape(
            fiona.transform.transform_geom(vector_crs, raster_crs, feature['geometry'], antimeridian_cutting=True))

        feature_metrics = {
            'outside': False
        }

        # Compare feature and raster bboxes to determine if feature is definitely outside.
        # If outside, no need to process.
        if not zone_geometry.intersects(raster_bounds):
            feature_metrics['outside'] = True
            stats[feature['id']] = feature_metrics
        else:

            feature_metrics['bands'] = {}

            # Figure out if the geometry is completely contained within the raster
            if contained:
                feature_metrics['contained'] = raster_bounds.contains(zone_geometry)

            # Collect all intersecting windows and create a new overall window
            # This new window is used to read a raster subset
            intersecting_windows = np.array(
                [i[1] for i in [window_ids[id] for id in window_index.intersection(zone_geometry.bounds)]])
            min_col = intersecting_windows[:, 0].min()
            max_col = intersecting_windows[:, 0].max()
            min_row = intersecting_windows[:, 1].min()
            max_row = intersecting_windows[:, 1].max()
            subset_window = ((min_col, max_col), (min_row, max_row))

            # Only the bands the user is interested in computing stats against are extracted
            # Be sure to create a new affine transformation with the upper left coordinates
            # for the subset raster

            raster_subset = raster.read(bands, window=subset_window)
            if not isinstance(raster_subset, np.ma.MaskedArray):
                raster_subset = np.ma.MaskedArray(raster_subset)
            _subset_ul_x, _subset_ul_y = raster.affine * (min_row, min_col)
            subset_affine = affine.Affine(
                raster.affine.a, raster.affine.b, _subset_ul_x,
                raster.affine.d, raster.affine.e, _subset_ul_y)

            # Rasterize the geometry and convert to a boolean array
            rasterized = rasterio.features.rasterize([shapely.geometry.mapping(zone_geometry)],
                                                     out_shape=raster_subset[0].shape,
                                                     transform=subset_affine,
                                                     all_touched=all_touched).astype(np.bool)

            # Compute stats for all specified bands
            # np_bidx is zero indexing
            band_map = dict(zip(bands, range(raster_subset.shape[0])))
            for bidx in bands:

                np_bidx = band_map[bidx]

                # Update mask and compute metrics
                masked_subset = np.ma.masked_array(
                    raster_subset[np_bidx].data, mask=raster_subset[np_bidx].mask + ~rasterized)
                feature_metrics['bands'][bidx] = {}
                for name, func in metrics.items():
                    if func is not None:
                        feature_metrics['bands'][bidx][name] = func(masked_subset)

                stats[feature['id']] = feature_metrics.copy()

    return stats


@click.command()
@click.argument('raster')
@click.argument('vector')
@click.option(
    '-b', '--bands',
    help="Bands to process as `1' or `1,2,3'."
)
@click.option(
    '-at', '--all-touched', is_flag=True,
    help="Enable 'all-touched' rasterization."
)
@click.option(
    '-c', '--contained', is_flag=True,
    help="Enable 'contained' test."
)
@click.option(
    '-npp', '--no-pretty-print', is_flag=True,
    help="Print stats JSON on one line."
)
@click.option(
    '--indent', type=click.INT, default=0,
    help="Pretty print indent."
)
def main(raster, vector, bands, contained, all_touched, no_pretty_print, indent):

    """
    Get raster stats for every feature in a vector datasource.

    Only compute against the first two bands:

        $ ./zonal-statistics.py sample-data/NAIP.tif \
                sample-data/polygon-samples.geojson -b 1,2

    Compute against all bands and also perform a check to see if a geometry is
    completely contained within the raster bbox:

        $ ./zonal-statistics.py sample-data/NAIP.tif \
                sample-data/polygon-samples.geojson --contained
    """

    if bands is not None:
        bands = sorted([int(i) for i in bands.split(',')])

    with fiona.drivers(), rasterio.drivers():
        with rasterio.open(raster) as src_r, fiona.open(vector) as src_v:

            results = zonal_stats_from_raster(src_v, src_r, bands=bands, contained=contained, all_touched=all_touched)
            if not no_pretty_print:
                results = pprint.pformat(results, indent=indent)
            click.echo(results)

    sys.exit(0)


if __name__ == '__main__':
    main()
