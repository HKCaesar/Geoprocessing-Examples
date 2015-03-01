#!/usr/bin/env python


"""
Convert data read by something like csv.DictReader to GeoJSON on the fly
"""


import csv
import logging
import json

import click
import fiona
import shapely.geometry
import shapely.wkt


log = logging.getLogger('csv_adapter')


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
    Generator to read a CSV containing vector data and convert to GeoJSON on
    the fly.  Geometry can be supplied as WKT, GeoJSON, or points with X and
    Y stored in different fields.

    The user supplies an iterable, like an instance of `csv.DictReader()`, that
    provides one dictionary per iteration that is converted on the fly to a
    GeoJSON object and yielded to the parent process.  The user can specify
    which field contains the geometry and what kind of geometry representation
    it contains.

    All keys except `None` or the geometry field are included in the output
    features but the user can specify what fields to include and what type
    they should be cast to via the `properties` parameter.  If set to `None`
    all fields are included in the output feature but if set to a dictionary
    where keys are fieldnames and values are functions then only the fields
    specified in the keys are included in the output.  The functions must
    take a single argument, return a single value, and be prepared to handle
    values contained in the associated field in the dictionaries yielded by
    the `dict_reader`.

    Helper functions have been included to make creating a `properties`
    definition containing `str`, `float`, or `int` easier.  They are prefixed
    with `helper_` and `helper_properties_def` takes a set of Fiona properties
    and returns a definition suitable for the `properties` parameter that uses
    the helper functions.

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
    geom_type, geom_field = geomtype_field.split(':')
    geom_type = geom_type.lower()
    
    # Fail fast in case streaming from a large file
    if geom_type not in _allowed_geomtypes:
        raise ValueError("Invalid geometry type - must be one of: {gt}".format(gt=_allowed_geomtypes))
    elif geom_type == 'xy' and ',' not in geom_field:
        raise ValueError("Geometry type is 'xy' and ',' does not appear in fieldnames: %s" % geom_field)

    for row_num, _row in enumerate(dict_reader):

        # Define the properties definition from the first row if the user didn't supply one
        if properties is None:
            properties = {field: helper_str for field in _row if field not in (None, geom_field)}

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
                geometry = shapely.geometry.mapping(shapely.wkt.loads(row[geom_field]))
            elif geom_type == 'geojson':
                geometry = json.loads(row[geom_field])
            elif geom_type == 'xy':
                geometry = {
                    "type": "Point",
                    "coordinates": [float(row[field]) for field in geom_field.split(',')]
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
                log.exception(e)
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
@click.argument('outfile', default='-', required=True)
@click.option(
    '-sf', '--skip-failures', type=click.BOOL,
    help="Skip failures encountered on read and write."
)
@click.option(
    '-r', '--reader', default='csv', type=click.Choice(['csv', 'json', 'newlinejson']),
    help="Input data format."
)
@click.option(
    '-f', '--format', '--driver', required=True,
    help="Output driver name."
)
@click.option(
    '-gf', '--geom-field', required=True,
    help="Geometry field definition as `format:field'."
)
@click.option(
    '-co', '--creation-option', multiple=True,
    help="Driver specific creation option as key=val."
)
@click.option(
    '-p', '--property-def', multiple=True,
    help="Schema property as field=def."
)
@click.option(
    '-crs', '--crs',
    help="Specify CRS of input data."
)
def main(infile, outfile, creation_option, skip_failures, reader, driver, geom_def, property_def, crs):

    """
    Convert delimited vector data to an OGR datasource.
    """

    def first_plus_iterator(first, iterator):
        if first:
            _first = first
            first = None
            yield _first
        else:
            for item in iterator:
                yield item

    creation_option = {co.split('=')[0]: co.split('=')[1] for co in creation_option}
    properties = {fd.split('=')[0]: fd.split('=')[1] for fd in property_def}

    # Load the input file into a generator that produces one GeoJSON feature per iteration
    if reader == 'csv':
        reader = csv.DictReader(infile),
    elif reader == 'json':
        reader = json.load(infile)
    elif reader == 'newlinejson':
        reader = _newlinejson_reader(infile)
    feature_generator = dict_reader_as_geojson(reader, geom_def, properties=helper_properties_def(properties),
                                               skip_failures=skip_failures)

    # Cache the first feature so that
    first_feature = next(feature_generator)
    schema = first_feature['geometry']['type']
    if crs.startswith('+'):
        schema['']
    with fiona.open(outfile, 'w', driver=driver, schema=None):
        pass


if __name__ == '__main__':
    main()
