#!/usr/bin/env python


"""
Stream geometry from stdin perform one or more topology operations and write
to stdout.  See main() docstring for more info.
"""


import json
import logging
import sys

import click
import shapely.geometry
from str2type import str2type


log = logging.getLogger('streaming-topology-operations')


@click.command()
@click.option(
    '-to', '--topology-operation', metavar="name:arg=val:arg...", multiple=True,
    help="Define a topology operation."
)
@click.option(
    '-sf', '--skip-failures', is_flag=True,
    help="Skip all failures."
)
def main(topology_operation, skip_failures):

    """
    Perform Shapely topology operations on GeoJSON features or geometries.

    Topology operations are methods or properties available to
    `shapely.geometry.base.BaseGeometry` that returns a geometry and does
    not require a geometry as an argument.

    By no means perfect or complete but multiple operations can be chained
    together with the `-to` flag.  The result from each operation is passed
    on to the next in the chain so you can do stuff like compute the centroid
    for every feature and then immediately buffer it and write that geometry
    to the output file.

    \b
    Buffer geometries 15 meters:
    \b
        $ fio cat sample-data/polygon-samples.geojson \\
            | streaming-topology-operations.py \\
                -to buffer:distance=15
    \b
    Compute geometry centroid and buffer that by 100 meters:
    \b
        $ fio cat sample-data/polygon-samples.geojson \\
            | streaming-topology-operations.py \\
                -to centroid -to buffer:distance=100
    \b
    Compute geometry centroid, buffer 20 meters, and compute envelope:
    \b
        $ fio cat sample-data/polygon-samples.geojson \\
            | streaming-topology-operations.py \\
                -to centroid -to buffer:distance=20 -to envelope
    \b
    Read, transform, write:
    \b
        $ fio cat sample-data/tl_2014_54037_roads.geojson \\
            | streaming-topology-operations.py -to buffer:distance=3 \\
            | fio load sample-data/buffered/tl_2014_54037_roads.geojson \\
                -f GeoJSON --sequence --src_crs EPSG:32618 --dst_crs EPSG:32618
    """

    # Parse topology operations into a more usable format
    topo_ops = []
    for definition in topology_operation:
        name = definition.split(':')[0]
        if len(definition.split(':')) > 1:
            topo_ops.append((name, {a.split('=')[0]: str2type(a.split('=')[1]) for a in definition.split(':')[1:]}))
        else:
            topo_ops.append((name, {}))

    for idx, item in enumerate(sys.stdin):
        try:
            item = json.loads(item)

            # Load geometry from either a GeoJSON feature or geometry object
            if item['type'] == 'Feature':
                feature = item
                geom = shapely.geometry.asShape(item['geometry'])
            else:
                feature = None
                geom = shapely.geometry.asShape(item)

            # Perform all topology operations in order
            for name, args in topo_ops:

                # Some operations don't take any arguments or are properties so to prevent triggering argument
                # errors don't even try giving arguments to an operation if the user didn't specify any.  Let
                # the user be responsible for researching the arguments.
                operation = getattr(geom, name)
                if not hasattr(operation, '__call__'):
                    geom = operation  # Operation was a property so we already have the value via getattr()
                elif not args:
                    geom = operation()
                else:
                    geom = operation(**args)

            # Print to stdout
            if feature is not None:
                feature['geometry'] = shapely.geometry.mapping(geom)
                click.echo(json.dumps(feature))
            else:
                pass
                click.echo(json.dumps(shapely.geometry.mapping(geom)))

        except Exception as e:
            if not skip_failures:
                raise e
            else:
                log.exception("Exception on row %s - %s" % (idx, e))


if __name__ == '__main__':
    main()
