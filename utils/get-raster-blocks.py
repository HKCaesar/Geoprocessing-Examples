#!/usr/bin/env python


from collections import OrderedDict
import sys

import fiona
import rasterio


def main(args):

    with fiona.drivers(), rasterio.drivers():
        schema = {
            'crs': 'EPSG:26918',
            'driver': 'GeoJSON',
            'schema': {
                'properties': {
                    'id': 'int'
                },
                'geometry': 'Polygon'
            }
        }

        with rasterio.open(sys.argv[1]) as src:
            for bidx in range(src.count):
                schema = {
                    'crs': src.crs,
                    'driver': 'ESRI Shapefile',
                    'schema': {
                        'properties': {
                            'x': 'int',
                            'y': 'int'
                        },
                        'geometry': 'Polygon'
                    }
                }
                outfile = ''.join([sys.argv[2], '-', str(bidx), '.shp'])
                with fiona.open(outfile, 'w', **schema) as dst:
                    for idx, window in enumerate(src.block_windows(bidx)):
                        win_x, win_y = window[0]
                        minimum, maximum = [src.affine * c for c in window[1]]
                        print(window[1], minimum, maximum)
                        x_min, y_min = minimum
                        x_max, y_max = maximum
                        dst.write({
                            'type': 'Feature',
                            'properties': {
                                'x': win_x,
                                'y': win_y
                            },
                            'geometry': {
                                'type': 'Polygon',
                                'coordinates': [[
                                        [x_min, y_min], [x_min, y_max], [x_max, y_max], [x_max, y_min]]
                                ]
                            }
                        })


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
