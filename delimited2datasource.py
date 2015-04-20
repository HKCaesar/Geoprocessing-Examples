#!/usr/bin/env python


"""
Convert data read by something like csv.DictReader to GeoJSON on the fly.

Names are overly verbose for ease of following what does what.
"""


import csv
import logging
import json
import sys

import click
import fiona
import fiona.transform
import shapely.geometry
import shapely.wkt


# Some OGR fields can contain valid data the exceeds the default CSV field size
csv.field_size_limit(sys.maxsize)


def helper_str(val):

    """
    Helper function for use with `dict_reader_as_geojson`.  Returns `None` if
    the input value is an empty string or `None`, otherwise the input value
    is cast to a string and returned.
    """

    if val is None or val == '':
        return None
    else:
        return str(val)


def helper_float(val):

    """
    Helper function for use with `dict_reader_as_geojson`.  Returns `None` if
    the input value is an empty string or `None`, otherwise the input value is
    cast to a float and returned.
    """

    if val is None or val == '':
        return None
    else:
        return float(val)


def helper_int(val):

    """
    Helper function for use with `dict_reader_as_geojson`.  Returns `None` if
    the input value is an empty string or `None`, otherwise the input value is
    cast to an int and returned.
    """

    if val is None or val == '':
        return None
    else:
        return int(val)


def helper_properties_def(properties):

    """
    Convert properties from a Fiona datasource to a definition that is usable by
    `dict_reader_as_geojson` and utilizes the helper functions for better value
    handling.

    Sample input:

        {
            'float_field': 'float:14',
            'int_field': 'int:8',
            'str_field': 'str:16'
        }

    Sample output:

        {
            'float_field': helper_float,
            'int_field': helper_int,
            'str_field': helper_str
        }

    Parameters
    ----------
    properties : dict
        Dictionary from a Fiona datasource.

    Returns
    -------
    dict
        Keys are fields from the input properties and vals are one of the three
        helper functions: helper_str, helper_int, helper_float.
    """

    return {field: globals()['helper_' + properties[field].split(':')[0]] for field in properties}


def dict_reader_as_geojson(dict_reader, geomtype_field, properties=None, skip_failures=False, empty_is_none=True):

    """
    A generator to read a CSV containing vector data and convert to GeoJSON on
    the fly.  Geometry can be supplied as WKT, GeoJSON, or points with X and
    Y stored in different fields.

    The user supplies an iterable, like an instance of `csv.DictReader()`, that
    provides one dictionary per iteration that is converted on the fly to a
    GeoJSON object and yielded to the parent process.  The user can specify
    the geometry field and what kind of geometry representation it contains.

    All keys except `None` or the geometry field are included in the output
    features but the user can take control over what fields are included and
    what type they are cast to via the `properties` parameter.  If set to `None`
    all fields are included in the output feature but if set to a dictionary
    where keys are fieldnames and values are functions then only the fields
    specified in the keys are included in the output.  The functions must
    take a single argument, return a single value, and be prepared to handle
    values contained in the associated field in the dictionaries yielded by
    the `dict_reader`.

    Helper functions have been included to make creating a `properties`
    definition containing `str`, `float`, or `int` easier.  They are prefixed
    with `helper_` and `helper_properties_def` takes a set of Fiona properties
    and returns a definition constructed with the helpers that is suitable for
    the `properties` parameter.  The primary job of these helper functions is to
    return `None` when they encounter an empty string or `None`.  If the standard
    Python `int, float, etc.` functions were used then exceptions would constantly
    be raised.

    Example `properties` definition:

        {
            'field1': float,
            'field2': helper_int,
            'field3': custom_function
        }

    Create a `properties` definition from a set of Fiona properties:

    Valid `geomtype_field` definitions:

        wkt:field
        geojson:field
        xy:x_field,y_field[,z_field]

    Parameters
    ----------
    dict_reader : csv.DictReader (or other iterable object returning dicts)
        A loaded instance configured with whatever settings the user desires.
    skip_failures : bool, optional
        If `True` and an exception is encountered while iterating over rows, that
        row will be skipped and the exception will be logged.  If `False` the
        exception is raised.
    geomtype_field : str
        Geometry type and field description.
    properties : dict, optional
        Keys are fieldnames in the CSV and vals are functions used to cast the
        values to a Python type.  If `None` then all fields are converted to
        strings.  Fields that are `None` or are specified in the
        `geomtype_field` argument are ignored even if they are specified in the
        `properties` parameter.

    Yields
    ------
    dict
        A GeoJSON feature.
    """

    _allowed_geomtypes = ('wkt', 'geojson', 'xy')

    # Parse the geometry field definition
    geom_type, geometry_field = geomtype_field.split(':')
    geom_type = geom_type.lower()
    
    # Fail fast in case streaming from a large file
    if geom_type not in _allowed_geomtypes:
        raise ValueError("Invalid geometry type - must be one of: {gt}".format(gt=_allowed_geomtypes))
    elif geom_type == 'xy' and ',' not in geometry_field:
        raise ValueError("Geometry type is 'xy' and ',' does not appear in fieldnames: %s" % geometry_field)

    for row_num, _row in enumerate(dict_reader):

        # Define the properties definition from the first row if the user didn't supply one
        if properties is None:
            properties = {field: helper_str for field in _row if field not in (None, geometry_field)}

        # skip_failures option only skips failures that occur within this block
        try:

            # Convert vals that are empty strings to None
            row = {}
            if empty_is_none:
                for k, v in _row.items():
                    if v == '':
                        v = None
                    row[k] = v
            else:
                row = _row

            # Convert string representation of geometry to a geojson object
            if geom_type == 'wkt':
                geometry = shapely.geometry.mapping(shapely.wkt.loads(row[geometry_field]))
            elif geom_type == 'geojson':
                geometry = json.loads(row[geometry_field])
            elif geom_type == 'xy':
                geometry = {
                    "type": "Point",
                    "coordinates": [float(row[field]) for field in geometry_field.split(',')]
                }
            else:
                raise ValueError("Invalid geometry type definition: %s" % geom_type)

            # Give the parent process a GeoJSON feature
            yield {
                "id": row_num,
                "type": "Feature",
                "geometry": geometry,
                "properties": {field: caster(row[field]) for field, caster in properties.items()}
            }

        # Encountered an error
        except Exception as e:
            if skip_failures:
                logging.exception(str(e))
            else:
                raise e


def _newlinejson_reader(infile):

    """
    Read a newline delimited JSON file

    Parameters
    ----------
    infile : file
        Open file-like object with read access.

    Yields
    ------
    dict or list
    """

    for line in infile:
        if line:
            yield json.loads(line)


@click.command()
@click.argument('infile', type=click.File(mode='r'), required=True)
@click.argument('outfile', required=True)
@click.option(
    '-sf', '--skip-failures', is_flag=True, type=click.BOOL,
    help="Skip failures encountered on read and write."
)
@click.option(
    '-r', '--reader', metavar='NAME', default='csv', type=click.Choice(['csv', 'json', 'newlinejson']),
    help="Input data format."
)
@click.option(
    '-f', '--format', '--driver', metavar='NAME',
    help="Output driver name."
)
@click.option(
    '-gf', '--geometry-field', metavar='NAME', required=True,
    help="Geometry field definition as `format:field'."
)
@click.option(
    '-co', '--creation-option', metavar='NAME=VAL', multiple=True,
    help="Driver specific creation option as key=val."
)
@click.option(
    '-p', '--property-definition', metavar='FIELD=TYPE:WIDTH', multiple=True,
    help="Schema property as field=def."
)
@click.option(
    '-s-crs', '--src_crs', metavar='CRS_DEF',
    help="Specify CRS of input data."
)
@click.option(
    '-d-crs', '--dst_crs', metavar='CRS_DEF',
    help="Specify CRS of output data."
)
@click.option(
    '-sl', '--skip-lines', metavar='N', type=click.INT, default=0,
    help="Skip N lines."
)
@click.option(
    '-ss', '--subsample', metavar='N', type=click.INT, default=0,
    help="Only process N lines."
)
@click.option(
    '-gt', '--geometry-type', metavar='TYPE',
    help="Specify geometry type for output layer."
)
def main(infile, outfile, creation_option, skip_failures, reader, driver, geometry_field, property_definition,
         src_crs, dst_crs, skip_lines, subsample, geometry_type):

    """
    Convert delimited vector data to an OGR datasource supported by Fiona.

    \b
    Print GeoJSON to stdout:
    \b
        $ delimited2datasource.py sample-data/WV.csv - -gf wkt:WKT
    \b
    Convert to a shapefile:
    \b
        $ delimited2datasource.py sample-data/WV.csv WV.shp -gf wkt:WKT
    \b
    Skip 5 lines and only process the next 10:
    \b
        $ delimited2datasource.py sample-data/WV.csv - -gf wkt:WKT -sl 5 -ss 10
    \b
    Reproject geometries:
    \b
        $ delimited2datasource.py sample-data/WV.csv - -gf wkt:WKT \\
            --src_crs EPSG:4269 --dst_crs EPSG:4326
    \b
    Only write two fields but convert one to an int:
    \b
        $ delimited2datasource.py sample-data/WV.csv - \\
            -gf wkt:WKT -p NAME=str -p STATEFP=int
    \b
    Read point data from an x and y columns while casting one field to int and one to float:
    \b
        $ delimited2datasource.py sample-data/WV.csv - \\
            -gf xy:centroid_x,centroid_y -p COUNTYFP=int -p ALAND=float
    """

    def first_plus_iterator(first, iterator):
        yielded_first = False
        for item in iterator:
            if not yielded_first:
                yielded_first = True
                yield first
            else:
                yield item

    if outfile == '-' and driver is not None or len(creation_option) > 0:
        click.echo("ERROR: Cannot specify driver or creation options if printing to stdout.", err=True)
        sys.exit(1)

    # Convert the input file into an iterable object
    if reader == 'csv':
        reader = csv.DictReader(infile)
    elif reader == 'json':
        reader = json.load(infile)
    elif reader == 'newlinejson':
        reader = _newlinejson_reader(infile)
    else:
        raise ValueError("Invalid reader: `%s'" % reader)

    # If the user doesn't supply any properties then the transformer will automatically generate them from the first
    # feature.  The first output GeoJSON feature can be cached and a set of Fiona properties can be constructed from it.
    # Kinda sketchy and could use a re-write
    properties = {p.split('=')[0]: p.split('=')[1] for p in property_definition}
    if not properties:
        generator_properties = None
    else:
        generator_properties = helper_properties_def(properties)
    feature_generator = dict_reader_as_geojson(reader, geometry_field, properties=generator_properties,
                                               skip_failures=skip_failures)

    # Cache the first feature so we can extract information to build the schema
    first_feature = next(feature_generator)

    # Build the schema for the output Fiona datasource
    schema = {co.split('=')[0]: co.split('=')[1] for co in creation_option}
    if geometry_type is None:
        geometry_type = first_feature['geometry']['type']
    if properties is None:
        properties = {p: 'str' for p in first_feature['properties'].keys()}
    schema.update(
        crs=dst_crs,
        driver=driver,
        schema={
            'geometry': geometry_type,
            'properties': properties
        }
    )
    with fiona.open(outfile, 'w', **schema) if outfile != '-' else sys.stdout as dst:

        for idx, feature in enumerate(first_plus_iterator(first_feature, feature_generator)):

            if idx >= skip_lines:

                # Skip failures block
                try:

                    feature['geometry'] = fiona.transform.transform_geom(
                        src_crs, dst_crs, feature['geometry'], antimeridian_cutting=True)

                    if dst is sys.stdout:
                        click.echo(json.dumps(feature))
                    else:
                        dst.write(feature)

                    # Subsample and skip lines are 1 indexing while idx is 0 indexing so add 1 for the comparison since
                    # its always off
                    if idx + 1 is subsample + skip_lines:
                        break

                except Exception as e:
                    if not skip_failures:
                        raise e

    sys.exit(0)


if __name__ == '__main__':
    main()
