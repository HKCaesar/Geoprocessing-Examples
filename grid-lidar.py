#!/usr/bin/env python


"""
LiDAR processing examples
"""


from __future__ import division

import sys

import affine
import click
import laspy.file
import numpy as np
import rasterio
import rasterio.warp
import scipy.interpolate


@click.command()
@click.argument('lidar')
@click.argument('raster')
@click.option(
    '-tr', '--target-res', nargs=2, metavar='X Y', type=click.FLOAT,
    help="Output cell size in georeferenced units."
)
@click.option(
    '-ts', '--target-size', nargs=2, metavar='COL ROW', type=click.INT,
    help="Output size in rows and columns."
)
@click.option(
    '-f', '--format', '--driver', metavar='NAME', default='GTiff',
    help="Output raster driver."
)
@click.option(
    '-co', '--creation-option', metavar='OPT=VAL', multiple=True,
    help="Raster creation options."
)
@click.option(
    '-crs', '--crs', required=True,
    help="LiDAR CRS."
)
@click.option(
    '-i', '--interpolation', type=click.Choice(['nearest', 'linear', 'cubic']), default='nearest',
    help="Interpolation method."
)
@click.option(
    '-kr', '--keep-return', type=click.INT, metavar='INT',
    help="Process points with the specified return number."
)
@click.option(
    '-kc', '--keep-class', type=click.INT, metavar='INT',
    help="Process points with the specified classification."
)
def rasterize_z(lidar, raster, target_res, target_size, crs, driver, creation_option, interpolation,
                keep_class, keep_return):

    """
    Grid LiDAR into a raster.

    Currently only Z values can be gridded and point filtering could be much
    more sophisticated but its a proof of concept.
    """

    # Validate arguments and convert to pixel space (Y, X)
    if len(target_res) is not 0:
        target_res = (-abs(target_res[1]), abs(target_res[0]))
    if len(target_size) is not 0:
        target_size = tuple(reversed(target_size))
    if len(target_res) is 0 and len(target_size) is 0:
        click.echo("ERROR: Need target resolution or target size.")
        sys.exit(1)
    elif len(target_res) is not 0 and len(target_size) is not 0:
        click.echo("ERROR: Cannot specify target resolution and target size.")
        sys.exit(1)
        
    with laspy.file.File(lidar) as las:

        if len(target_res) is not 0:
            n_cols = abs(int((las.x.max() - las.x.min()) / target_res[0]))
            n_rows = abs(int((las.y.max() - las.y.min()) / target_res[1]))
        elif len(target_size) is not 0:
            target_res = (-abs((las.y.max() - las.y.min()) / target_size[0]), abs((las.x.max() - las.x.min()) / target_size[0]))
            n_rows, n_cols = target_size
        geotransform = (las.x.min(), target_res[1], 0, las.y.max(), 0, target_res[0])
        meta = {
            'count': 1,
            'crs': crs,
            'dtype': rasterio.float32,
            'affine': affine.Affine.from_gdal(*geotransform),
            'driver': driver,
            'height': n_rows,
            'width': n_cols,
            'nodata': -9999,
            'transform': affine.Affine.from_gdal(*geotransform)
        }
        meta.update({co.split('=')[0]: co.split('=')[1] for co in creation_option})
        with rasterio.open(raster, 'w', **meta) as raster:

            X = las.x
            Y = las.y
            Z = las.z

            # This could be WAY fancier
            if keep_class is not None:
                classification = las.classification
                X = X[classification == keep_class]
                Y = Y[classification == keep_class]
                Z = Z[classification == keep_class]
            if keep_return is not None:
                return_num = las.return_num
                X = X[return_num == keep_return]
                Y = Y[return_num == keep_return]
                Z = Z[return_num == keep_return]

            # Ideally the user would have access to a triangulation routine as well but this is the
            # quick and dirty method.  Triangulation would let the user specify max leg length for
            # better anti-aliasing and better vegetation representation if it supports finding the nearest
            # neighbor in 3D instead of just 2D.  Nodata areas are filled so water contains a lot of nonsense
            # values, which isn't terrible except that they stretch out from shoreline vegetation to create large
            # elevated triangles.
            xi = np.linspace(X.min(), X.max(), raster.meta['width'])
            yi = np.linspace(Y.min(), Y.max(), raster.meta['height'])
            gridded = scipy.interpolate.griddata(
                points=(X, Y),
                values=Z,
                xi=(xi[None, :], yi[:, None]),
                method=interpolation,
                fill_value=raster.meta['nodata'],
            )[::-1]  # For some reason the entire array is flipped across the the X axis???
            raster.write_band(1, gridded.astype(raster.meta['dtype']))


if __name__ == '__main__':
    rasterize_z()
