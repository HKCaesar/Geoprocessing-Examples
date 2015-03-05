#!/usr/bin/env python


"""
Write raster windows to a vector file.  Hard coded to write a directory of
shapefiles
"""


from collections import OrderedDict
import sys

import fiona
import rasterio


def main(args):

    if len(args) != 2:
        print("Usage: raster directory/prefix")
        return 1

    with fiona.drivers(), rasterio.drivers():
        with rasterio.open(sys.argv[1]) as src:
            for bidx in range(src.count):
                schema = {
                    'crs': src.crs,
                    'driver': 'ESRI Shapefile',
                    'schema': {
                        'properties': OrderedDict({
                            'x': 'int',
                            'y': 'int',
                            'col_min': 'int:8',
                            'col_max': 'int:8',
                            'row_min': 'int:8',
                            'row_max': 'int:8',
                        }),
                        'geometry': 'Polygon'
                    }
                }
                with fiona.open(sys.argv[2] + str(bidx) + '.shp', 'w', **schema) as dst:
                    for idx, window in enumerate(src.block_windows(bidx)):
                        _row, _col = window[1]
                        row_min, row_max = _row
                        col_min, col_max = _col
                        col_min, row_min = src.affine * (col_min, row_min)
                        col_max, row_max = src.affine * (col_max, row_max)
                        coordinates = [col_min, row_max], [col_min, row_min], [col_max, row_min], [col_max, row_max]
                        dst.write({
                            'type': 'Feature',
                            'properties': {
                                'x': window[0][0],
                                'y': window[0][1],
                                'col_min': col_min,
                                'col_max': col_max,
                                'row_min': row_min,
                                'row_max': row_max
                            },
                            'geometry': {
                                'type': 'Polygon',
                                'coordinates': [coordinates]
                            }
                        })

    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
