#!/usr/bin/env python


"""
Compute zonal statistics for every feature in a vector datasource across every
band in a raster datasource.

Names are overly verbose for ease of following what does what.
"""


import sys

import click
import fiona
import rasterio


def zonal_stats(vector, raster, window_band=0):
    stats = {}
    for window in raster.block_windows(window_band):
        for feature in vector.items(bbox):

def main():
    pass


if __name__ == '__main__':
    main()
