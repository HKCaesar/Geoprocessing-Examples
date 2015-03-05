#!/usr/bin/env python


"""
Stream geometry from stdin perform one or more topology operations and write
to stdout.
"""


import json
import sys

import click
import shapely.geometry
from str2type import str2type
try:
    from fiona.transform import transform_geom
except ImportError:
    transform_geom = None


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
    Perform topology operations on GeoJSON features or geometries.
    """

    # Parse topology operations into a more usable format
    topo_ops = {}
    for definition in topology_operation:
        name = definition.split(':')[0]
        if len(definition.split(':')) > 1:
            topo_ops[name] = {a.split('=')[0]: str2type(a.split('=')[1]) for a in definition.split(':')[1:]}
        else:
            topo_ops[name] = {}
    if transform_geom is None and 'transform_geom' in topo_ops:
        click.echo("ERROR: `transform_geom' topology operation specified but could not import Fiona.", err=True)
        sys.exit(1)

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
            for name, args in topo_ops.items():
                if name == 'transform_geom':
                    args.update(geom=shapely.geometry.mapping(geom))
                    geom = shapely.geometry.asShape(transform_geom(**args))
                else:

                    # Some operations don't take any arguments so to prevent triggering argument errors don't even
                    # try giving arguments to an operation if the user didn't specify any.  Let the user be responsible
                    # for researching the arguments
                    if not args:
                        geom = getattr(geom, name)()
                    else:
                        geom = getattr(geom, name)(**args)

            # Print to stdout
            if feature is not None:
                feature['geometry'] = shapely.geometry.mapping(geom)
                click.echo(json.dumps(feature))
            else:
                click.echo(json.dumps(shapely.geometry.mapping(geom)))

        except Exception as e:
            if not skip_failures:
                raise e  # Don't skip failures
            else:
                click.echo("Exception on row %s - %s" % (idx, e), err=True)

    sys.exit(0)


if __name__ == '__main__':
    main()