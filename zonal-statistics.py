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


def raster_window_to_geometry(block, transform):

    """
    Given a raster window/block and affine from the same datasource, return
    the window as a GeoJSON geometry in coordinate space.

    Parameters
    ----------
    block : list, tuple
        The window portion of a block item returned by `<raster>.block_windows()`.
    affine : affine.Affine()
        An affine transformation instance from the same raster.

    Returns
    -------
    dict
        GeoJSON geometry object in coordinate space.
    """

    _y, _x = block
    y_min, y_max = _y
    x_min, x_max = _x
    return {
        'geometry': {
            'type': 'Polygon',
            'coordinates': [[transform * pair for pair in
                            [x_min, y_min], [x_min, y_max], [x_max, y_max], [x_max, y_min]]]
        }
    }


def zonal_stats_from_raster(vector, raster, bands=None, contained=True, all_touched=None, custom=None, window_band=0):

    """
    Compute zonal statistics for each input feature across all bands of an input
    raster.  God help ye who supply non-block encoded rasters...

    User can supply their own statistics functions by passing a dictionary to
    the `custom` parameter.  Keys will be used to apply a name for each custom
    metric to the output statistics and values must be functions that accept a
    masked array and be prepare to compute metrics across unmasked values.

    In order to handle raster larger than available memory and vector datasets
    containing a large number of features, the minimum bounding box for each
    feature's geometry is computed and all intersecting raster windows are read.
    The inverse of the geometry is burned into this subset's mask and

    Example output:




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
        Supply custom functions.
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
        {
            'id': int,  # Vector feature ID
            'outside': bool,  # True if feature is entirely outside raster
            'contained': bool,  # True if feature is partially outside raster.
                                # If `contained=False` it will not be included
                                # in the output
            'mean': float,
            'min': float or int,
            'max': float or int,
            'sum': float or int,
            'std': float,
        }
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

    # User specified a single band but not in a list/tuple - wrap in a list
    elif isinstance(bands, int):
        bands = [bands]

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

            # Compute stats for all
            # np_bidx is zero indexing
            for np_bidx in range(raster_subset.shape[0]):

                # Update mask and compute metrics
                masked_subset = np.ma.masked_array(
                    raster_subset[np_bidx].data, mask=raster_subset[np_bidx].mask + ~rasterized)
                # raster_subset[np_bidx].__setmask__(raster_subset[np_bidx].mask + ~rasterized)
                feature_metrics['bands'][np_bidx + 1] = {}
                for name, func in metrics.items():
                    if func is not None:
                        feature_metrics['bands'][np_bidx + 1][name] = func(masked_subset)

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
def main(raster, vector, bands, contained, all_touched):

    if bands is not None:
        bands = bands.split(',')

    with fiona.drivers(), rasterio.drivers():
        with rasterio.open(raster) as src_r, fiona.open(vector) as src_v:
            results = zonal_stats_from_raster(src_v, src_r, bands=bands, contained=contained, all_touched=all_touched)
            click.echo(pprint.pformat(results, indent=4, depth=4))

    sys.exit(0)


if __name__ == '__main__':
    main()
