#!/usr/bin/env python

from __future__ import division

from collections import OrderedDict
import os
import sys

import click
import fiona as fio
import rasterio as rio


def convert_coords(lon, lat):
    lon_dir = lon[0]
    lat_dir = lat[0]
    lon = lon[1:].replace('(', '').replace(')', '')
    lat = lat[1:].replace('(', '').replace(')', '')
    lon_deg, lon_min, lon_sec = lon.split()
    lat_deg, lat_min, lat_sec = lat.split()
    o_lon = int(lon_deg) + (float(lon_min) / 60) + (float(lon_sec) / 3600)
    o_lat = int(lat_deg) + (float(lat_min) / 60) + (float(lat_sec) / 3600)
    if lon_dir.lower() == 'w':
        o_lon = -o_lon
    if lat_dir.lower() == 's':
        o_lat = -o_lat
    return o_lon, o_lat


@click.command()
@click.argument('top_dir')
@click.argument('outfile')
@click.option(
    '-f', '--format', '--driver', 'driver_name', metavar='NAME', required=True,
    help="Output driver."
)
def main(top_dir, outfile, driver_name):

    """
    Extract location information from photos and convert to a vector.

    Designed to be run against `"~/Pictures/iPhoto Library/Originals/"`, only
    looks for jpg, JPG, jpeg, and JPEG, only writes to EPSG:4326 and requires
    4 tags to be present in the EXIF data:

    \b
        GPSLongitudeRef
        GPSLongitude
        GPSLatitudeRef
        GPSLatitude
    """

    if raw_input("Continue (y/n)?  This is supposed to only read data but its untested so don't point it to critical photos.") != 'y':
        click.echo("Exiting.")
        sys.exit(1)
    if raw_input("Are you sure? (y/n):") != 'y':
        click.echo("Exiting.")
        sys.exit(1)

    # Crawl subdirectories and get input files
    click.echo("Getting input files...")
    a_f = []
    for directory, _, files in os.walk(os.path.expanduser(top_dir)):
        for f in files:
            if 'jpg' in f or 'JPG' or 'jpeg' in f or 'JPEG' in f:
                a_f.append(os.path.join(directory, f))
    click.echo("    Found %s" % len(a_f))

    # Extract all the image tags
    click.echo("Extracting image tags...")
    all_tags = []
    with click.progressbar(a_f) as a_f:
        for jpg in a_f:
            with fio.drivers(), rio.drivers():
                try:
                    with rio.open(jpg) as src:
                        tags_as_dict = {k.strip().split('_')[1]: v.strip() if v.strip() != '' else None for k, v in src.tags().items()}
                        if len(tags_as_dict) is not 0:
                            if tags_as_dict['GPSLongitudeRef'] is not None and \
                               tags_as_dict['GPSLongitude'] is not None and \
                               tags_as_dict['GPSLatitudeRef'] is not None and \
                               tags_as_dict['GPSLatitude'] is not None:
                                all_tags.append((jpg, tags_as_dict))
                except:
                    pass
    click.echo("    Found %s" % len(all_tags))

    # Get a lit of unique tags that can be used to create a fiona schema
    unique_tags = []
    for _, t in all_tags:
        unique_tags += t.keys()
    unique_tags = sorted(list(set(unique_tags)))
    click.echo("Creating output vector datasource...")

    meta = {
        'schema': {
            'geometry': 'Point',
            'properties': OrderedDict(filepath='str', name='str', **{t: 'str' for t in unique_tags})
        },
        'driver': driver_name,
        'crs': 'EPSG:4326'
    }
    with fio.drivers(), rio.drivers():
        with fio.open(outfile, 'w', **meta) as dst:
            click.echo("Processing output vector...")
            with click.progressbar(all_tags) as all_tags:
                for filepath, tag in all_tags:
                    props = dst.schema['properties'].keys()
                    props += ['filepath', 'name']
                    tag['filepath'] = filepath
                    tag['name'] = os.path.basename(filepath)
                    x, y = convert_coords(
                        tag['GPSLongitudeRef'] + tag['GPSLongitude'], tag['GPSLatitudeRef'] + tag['GPSLatitude'])
                    dst.write(
                        {
                            'geometry': {
                                'type': 'Point',
                                'coordinates': [x, y]
                            },
                            'properties': OrderedDict(**{p: tag.get(p, None) for p in props})
                        }
                    )
    sys.exit(0)

if __name__ == '__main__':
    main()