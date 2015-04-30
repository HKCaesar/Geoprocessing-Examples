#!/usr/bin/env python


"""
Compute a density raster for input geometries or sum a property.
"""


from __future__ import division

import affine
import click
import fiona as fio
import numpy as np
import rasterio as rio
import rasterio.dtypes as rio_dtypes
from rasterio.features import rasterize
import str2type.ext


def cb_res(ctx, param, value):

    """
    Click callback to handle ``--resolution`` syntax and validation.

    Parameter
    ---------
    ctx : click.Context
        Ignored
    param : click.Parameter
        Ignored
    value : tuple
        Tuple of values from each instance of `--resolution`.

    Returns
    -------
    tuple
        First element is pixel x size and second is pixel y size.
    """

    if len(value) > 2:
        raise click.BadParameter('target can only be specified once or twice.')
    elif len(value) is 2:
        return tuple(abs(v) for v in value)
    elif len(value) is 1:
        return value[0], value[0]
    else:
        raise click.BadParameter('bad syntax: {0}'.format(value))


def cb_bbox(ctx, param, value):

    """
    Click callback to handle ``--bbox`` syntax and validation.

    Parameters
    ----------
    ctx : click.Context
        Ignored.
    param : click.Parameter
        Ignored.
    value : tuple
        x_min, y_min, x_max, y_max

    Raises
    ------
    click.BadParameter

    Returns
    -------
    tuple
        (x_min, y_min, x_max, y_max)
    """

    if not value:
        return None

    bbox = value
    x_min, y_min, x_max, y_max = bbox

    if (x_max < x_min) or (y_max < y_min):
        raise click.BadParameter('min exceeds max for one or more dimensions: {0}'.format(' '.join(bbox)))

    return bbox


@click.command()
@click.argument('infile')
@click.argument('outfile')
@click.option(
    '-f', '--format', '--driver', 'driver_name', metavar='NAME', default='GTiff',
    help="Output raster driver."
)
@click.option(
    '-c', '--creation-option', metavar='NAME=VAL', multiple=True,
    callback=str2type.ext.click_cb_key_val, help="Output raster creation options."
)
@click.option(
    '-t', '--output-type', type=click.Choice(rio_dtypes.typename_fwd.values()),
    metavar='NAME', default='Float32',
    help="Output raster type.  Defaults to `Float32' but must support the value "
         "accessed by --property if it is supplied."
)
@click.option(
    '-r', '--resolution', type=click.FLOAT, multiple=True, required=True,
    callback=cb_res, help="Target resolution in georeferenced units.  Assumes square "
                          "pixels unless specified twice.  -tr 1 -tr 2 yields pixels "
                          "that are 1 unit wide and 2 units tall."
)
@click.option(
    '-n', '--nodata', type=click.FLOAT, default=0.0,
    help="Nodata value for output raster."
)
@click.option(
    '-l', '--layer', 'layer_name', metavar='NAME',
    help="Name of input layer to process."
)
@click.option(
    '-p', '--property', 'property_name', metavar='NAME',
    help="Property to sum.  Defaults to density."
)
@click.option(
    '-a', '--all-touched', is_flag=True,
    help="Enable all touched rasterization."
)
@click.option(
    '--bbox', metavar='X_MIN Y_MIN X_MAX Y_MAX', nargs=4, callback=cb_bbox,
    help='Only process data within the specified bounding box.'
)
def main(infile, outfile, creation_option, driver_name, output_type, resolution, nodata, layer_name,
         property_name, all_touched, bbox):

    """
    Creation a geometry density map or sum a property.

    When summing a property every pixel that intersects a geometry has the value
    of the specified property added to the pixel, which also means that negative
    values will be subtracted - the overall net value is written to the output
    raster.

    Given two partially overlapping polygons A and B where A has a value of 1
    and B has a value of 2, pixels in the overlapping area will have a value of
    3, pixels in polygon A will have a value of 1, and pixels in polygon B will
    have a value of 2.  See below:

    \b
    Sum a property:
    \b
            A = 1
            B = 2
    \b
                    B
                    +------------+
            A       |222222222222|
            +-------+---+22222222|
            |1111111|333|22222222|
            |1111111|333|22222222|
            |1111111+---+--------+
            +-----------+

    \b
    Compute density:
    \b
                    B
                    +------------+
            A       |111111111111|
            +-------+---+11111111|
            |1111111|222|11111111|
            |1111111|222|11111111|
            |1111111+---+--------+
            +-----------+
    \b
    Create a point density raster at a 10 meter resolution:
    \b
        $ summation-raster.py sample-data/point-sample.geojson OUT.tif \\
            --creation-option TILED=YES \\
            --resolution 10
    \b
    Sum a property at a 100 meter resolution
    \b
        $ summation-raster.py sample-data/point-sample.geojson OUT.tif \\
            --creation-option TILED=YES \\
            --resolution 100 \\
            --property ID

    \b
    NOTE: Point layers work well but other types are raising the error below. All
          geometry types will work once this is fixed.

        Assertion failed: (0), function query, file AbstractSTRtree.cpp, line 285.
        Abort trap: 6
    """

    x_res, y_res = resolution

    with fio.open(infile, layer=layer_name) as src:

        if property_name is not None and src.schema['properties'][property_name].split(':')[0] == 'str':
            raise click.BadParameter("Property `%s' is an invalid type for summation: `%s'"
                                     % (property_name, src.schema['properties'][property_name]))

        v_x_min, v_y_min, v_x_max, v_y_max = src.bounds if not bbox else bbox
        raster_meta = {
            'count': 1,
            'crs': src.crs,
            'driver': driver_name,
            'dtype': output_type,
            'affine': affine.Affine.from_gdal(*(v_x_min, x_res, 0.0, v_y_max, 0.0, -y_res)),
            'width': int((v_x_max - v_x_min) / x_res),
            'height': int((v_y_max - v_y_min) / y_res),
            'nodata': nodata
        }
        raster_meta['transform'] = raster_meta['affine']
        raster_meta.update(**creation_option)

        with rio.open(outfile, 'w', **raster_meta) as dst:

            num_blocks = len([bw for bw in dst.block_windows()])
            with click.progressbar(dst.block_windows(), length=num_blocks) as block_windows:
                for _, window in block_windows:

                    ((row_min, row_max), (col_min, col_max)) = window
                    x_min, y_min = dst.affine * (col_min, row_max)
                    x_max, y_max = dst.affine * (col_max, row_min)

                    block_affine = dst.window_transform(window)

                    data = np.zeros((row_max - row_min, col_max - col_min))

                    for feat in src.filter(bbox=(x_min, y_min, x_max, y_max)):
                        if property_name is None:
                            add_val = 1
                        else:
                            add_val = feat['properties'][property_name]
                            if add_val is None:
                                add_val = 0

                        data += rasterize(
                            shapes=[feat['geometry']],
                            out_shape=data.shape,
                            fill=dst.nodata,
                            transform=block_affine,
                            all_touched=all_touched,
                            default_value=add_val,
                            dtype=rio.float64
                        )
                    dst.write(data.astype(dst.meta['dtype']), indexes=1, window=window)


if __name__ == '__main__':
    main()
