#!/usr/bin/env python


"""
Compute zonal statistics for every feature in a vector datasource across every
band in a raster datasource.

Names are overly verbose for ease of following what does what.
"""


import pprint
import sys

import click
import fiona
import fiona.transform
import numpy as np
import rasterio
import rasterio.features
import rtree.index
import shapely.geometry


def raster_window_to_geometry(block, affine):

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
            'coordinates': [[affine * pair for pair in
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


    if all_touched is None and 'Point' in vector.schema['geometry'] or 'Line' in vector.schema['geometry']:
        all_touched = True

    if bands is None:
        bands = range(1, raster.count + 1)
    elif len(bands) is 1:
        bands = [bands]

    stats = {}

    raster_crs = raster.crs
    vector_crs = vector.crs
    _r_min_x, _r_min_y, _r_max_x, _r_max_y = raster.bounds
    raster_bounds = shapely.geometry.Polygon(
        [[_r_min_x, _r_min_y], [_r_min_x, _r_max_y], [_r_max_x, _r_max_y], [_r_max_x, _r_min_y]]
    )

    # Create a spatial index consisting of the raster block's in the coordinate space
    window_index = rtree.index.Index()
    window_ids = {}
    for idx, block in enumerate(raster.block_windows(window_band)):
        _y, _x = block[1]
        y_min, y_max = raster.affine * _y
        x_min, x_max = raster.affine * _x
        window_index.insert(idx, (x_min, y_min, x_max, y_max))
        window_ids[idx] = block

    for feature in vector:

        # Reproject feature's geometry to the raster's CRS and convert to a shapely geometry
        geometry = shapely.geometry.asShape(
            fiona.transform.transform_geom(vector_crs, raster_crs, feature['geometry'], antimeridian_cutting=True))

        feature_stats = {
            'outside': False
        }

        # Compare feature and raster bboxes to determine if feature is definitely outside.
        # If outside, no need to process.
        if not geometry.intersects(raster_bounds):
            feature_stats['outside'] = True
            stats[feature['id']] = feature_stats
        else:

            feature_stats['bands'] = {}

            # Figure out if the geometry is completely contained within the raster
            if contained:
                feature_stats['contained'] = raster_bounds.contains(geometry)

            # Note that the first element of the array is immediately extracted, otherwise the shape would be
            # 1 dimensional and the first array would be examined 4 more times.
            iw = [window_ids[id] for id in window_index.intersection(geometry.bounds)]
            intersecting_windows = np.array([i[1] for i in iw])
            min_col = intersecting_windows[:, 0].min()
            max_col = intersecting_windows[:, 0].max()
            min_row = intersecting_windows[:, 1].min()
            max_row = intersecting_windows[:, 1].max()
            subsample_window = ((min_col, max_col), (min_row, max_row))
            raster_subset = raster.read(range(1, raster.count + 1), window=subsample_window)
            rasterized = rasterio.features.rasterize([shapely.geometry.mapping(geometry)], raster_subset[:, 0].shape,
                                                     all_touched=all_touched).astype(np.bool)
            print(feature['id'])
            print([i[0] for i in iw])
            print(geometry.bounds)
            return
            # print(raster_subset.shape)
            # print(rasterized.shape)
            for bidx in bands:
                fully_masked = raster_subset[:, bidx + 1]
                fully_masked.mask = (rasterized is False) & (fully_masked.mask is True)

                feature_stats['bands'][bidx] = {}

                for name, func in metrics.items():
                    if func is not None:
                        feature_stats['bands'][bidx][name] = func(fully_masked)

                stats[feature['id']] = feature_stats

    return stats


@click.command()
@click.argument('vector')
@click.argument('raster')
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
            click.echo(pprint.pformat(results, indent=4))

    sys.exit(0)


if __name__ == '__main__':
    main()
