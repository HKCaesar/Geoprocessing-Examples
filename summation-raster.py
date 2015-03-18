#!/usr/bin/env python


from __future__ import division

import click
import affine
import fiona as fio
import numpy as np
import rasterio as rio
import rasterio.dtypes as rio_dtypes
from rasterio.features import rasterize
from shapely.geometry import asShape


@click.command()
@click.argument('infile')
@click.argument('outfile')
@click.option(
    '-f', '--format', '--driver', 'driver_name', metavar='NAME', default='GTiff',
    help="Output raster driver."
)
@click.option(
    '-co', '--creation-option', metavar='NAME=VAL', multiple=True,
    help="Output raster creation options."
)
@click.option(
    '-ot', '--output-type', type=click.Choice(rio_dtypes.typename_fwd.values()), metavar='NAME', default='Float32',
    help="Output raster type.  Defaults to `Float32' but must support the value "
         "accessed by --property if it is supplied."
)
@click.option(
    '-tr', '--target-resolution', type=click.FLOAT, multiple=True, required=True,
    help="Target resolution in georeferenced units.  Assumes square pixels unless "
         "specified twice.  -tr 1 -tr 2 yields pixels that are 1 unit wide and 2 "
         "units tall."
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
    '-at', '--all-touched', is_flag=True,
    help="Enable all touched rasterization for non-point layers."
)
def main(infile, outfile, creation_option, driver_name, output_type, target_resolution, nodata, layer_name,
         property_name, all_touched):

    """
    geotransform = (-237481.5, 425.0, 0.0, 237536.4, 0.0, -425.0)

    In [2]: import affine

    In [3]: a = affine.Affine.from_gdal(*geotransform)

    In [4]: a
    Out[4]:
    Affine(425.0, 0.0, -237481.5,
           0.0, -425.0, 237536.4)
    """

    target_resolution = [abs(v) for v in target_resolution]
    if len(target_resolution) is 1:
        x_res = target_resolution[0]
        y_res = target_resolution[0]
    elif len(target_resolution) is 2:
        x_res, y_res = target_resolution
    else:
        raise ValueError("Can only specify target resolution twice - receive %s values: %s"
                         % (len(target_resolution), target_resolution))

    with fio.open(infile, layer=layer_name) as src:

        # Fail fasts
        if property_name is not None and src.schema['properties'][property_name].split(':')[0] == 'str':
            raise TypeError("Property `%s' is an invalid type for summation: `%s'"
                            % (property_name, src.schema['properties'][property_name]))

        v_x_min, v_y_min, v_x_max, v_y_max = src.bounds
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
        raster_meta.update(**{co.split('=')[0]: co.split('=')[1] for co in creation_option})

        with rio.open(outfile, 'w', **raster_meta) as dst:

            num_blocks = len([_ for _ in dst.block_windows()])

            with click.progressbar(dst.block_windows(), length=num_blocks) as block_windows:

                for pos, ((row_min, row_max), (col_min, col_max)) in block_windows:
                    x_min, y_min = dst.affine * (col_min, row_max)
                    x_max, y_max = dst.affine * (col_max, row_min)

                    block = np.zeros((row_max - row_min, col_max - col_min))

                    for feature in src.filter(bbox=(x_min, y_min, x_max, y_max)):

                        if property_name is not None:
                            add_val = feature['properties'][property_name]
                        else:
                            add_val = 1

                        geom_type = feature['geometry']['type']
                        if 'Point' in geom_type:
                            for point in feature['geometry']['coordinates'] if geom_type == 'MultiPoint' else [feature['geometry']['coordinates']]:
                                point_col, point_row = (int(_i) for _i in ~dst.affine * point[:2])
                                block[point_row - row_min][point_col - col_min] += add_val
                        else:
                            for geometry in feature['geometry']['coordinates'] if 'Multi' not in geom_type else [feature['geometry']['coordinates']]:

                                # Clip the input geometry to the raster block window
                                window_geometry = asShape({
                                        'type': 'Polygon',
                                        'coordinates': [[[x_min, y_min], [x_min, y_max], [x_max, y_max], [x_max, y_min]]]
                                    })
                                clipped = asShape(geometry).intersection(window_geometry)

                                # Create a new affine transformation for this subset window
                                _subset_ul_x, _subset_ul_y = dst.affine * (row_min, col_min)
                                subset_affine = affine.Affine(
                                    dst.affine.a, dst.affine.b, _subset_ul_x,
                                    dst.affine.d, dst.affine.e, _subset_ul_y)

                                # Rasterize polygon
                                rasterizd = rasterize(
                                    shapes=[clipped],
                                    out_shape=block.shape,
                                    transform=subset_affine,
                                    all_touched=all_touched,
                                    default_value=1 if property_name is None else feature['properties'][property_name],
                                    dtype=dst.meta['dtype']
                                )

                                block += rasterizd

                        block[block == 0.0] = nodata
                        dst.write_band(1, block.astype(dst.meta['dtype']), window=((row_min, row_max), (col_min, col_max)))


if __name__ == '__main__':
    main()
