import json
from esridump.dumper import EsriDumper
import os
from config.config_crashes import (ALL_CRASHES_SERVICE, PED_BIKE_QUERY)
from PMT import RAW
from pathlib import Path

GITHUB = True


def download_bike_ped_crashes(
        all_crashes_url=ALL_CRASHES_SERVICE,
        fields='ALL',
        where_clause=PED_BIKE_QUERY,
        out_crs='4326',
        out_path=RAW,
        out_name="bike_ped_crashes_raw.geojson"):
    """
    Reads in a feature service url and filters based on the query and
    saves geojson copy of the file to the specified output location

    Parameters
    ----------
    all_crashes_url: String, Url path to the all crashes layer
    fields: List, a comma-separated list of fields to request from the server
    where_clause: dict, a dictionary key of 'where' with the value being the intended filter
    out_crs: String, EPSG code used to define output coordinates
    out_path: Path, Directory where file will be stored
    out_name: String, The name of the output geojson file.

    Returns
    -------
    None, A geojson file of bike and pedestrian crashes is saved at '{out_path}/{out_name}'
    """
    # handle an option to limit fields returned
    if fields != 'ALL':
        if isinstance(fields, list):
            requested_fields = fields
        else:
            requested_fields = fields.split(',')
    else:
        requested_fields = None

    # read data from feature server
    dumper = EsriDumper(url=all_crashes_url,
                        extra_query_args=where_clause,
                        fields=requested_fields,
                        outSR=out_crs)

    # write out data from server to geojson
    out_file = os.path.join(out_path, out_name)
    with open(out_file, 'w') as dst:
        dst.write('{"type":"FeatureCollection","features":[\n')
        feature_iter = iter(dumper)
        try:
            feature = next(feature_iter)
            while True:
                dst.write(json.dumps(feature))
                feature = next(feature_iter)
                dst.write(',\n')
        except StopIteration:
            dst.write('\n')

        dst.write(']}')


if __name__ == "__main__":
    if GITHUB:
        ROOT = r'K:\Projects\MiamiDade\PMT\Data'
        RAW = str(Path(ROOT, 'Raw'))
    out_path = str(Path(RAW, "Safety_Security", "Crash_Data"))
    out_name = "bike_ped.geojson"
    download_bike_ped_crashes(
        all_crashes_url=ALL_CRASHES_SERVICE, fields='ALL', where_clause=PED_BIKE_QUERY,
        out_crs='4326', out_path=out_path, out_name=out_name)