# -*- encoding: utf-8 -*-

"""
Copyright (C) 2014-2018 OceanDataLab

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import os
import logging
import datetime
try:
    import simplejson as json
except ImportError:
    import json
from syntool_exporter.db import create_product, create_product_table
from syntool_exporter.db import create_dataset, upsert, create_missing_table
from syntool_exporter.db import Product

logger = logging.getLogger(__name__)
TIME_FMT = '%Y-%m-%d %H:%M:%S'

class MissingInputFile(OSError):
    """Exception triggered when one of the input files cannot be found on the
    system."""
    def __init__(self, path):
        self.path = path


# -----------------------------------------------------------------------------
def init_mysql(exporter, output_path):
    """"""
    sql = create_missing_table(Product)
    exporter(sql, output_path)


def convert_to_mysql(exporter, output_path, metadata_files, chunk_size,
                     syntool_id=None):
    """Export a list of datasets metdata to a single SQL file.
    Using this method when there are a lot of datasets to export gives a huge
    performance boost because it requires only one connection to the MySQL
    server.
    """
    fields = ('dataset_name', 'relative_path', 'begin_datetime',
              'end_datetime', 'min_zoom_level', 'max_zoom_level',
              'resolutions', 'bbox_text', 'bbox_geometry', 'shape_text',
              'shape_geometry')
    stmt_footer = upsert(fields[1:])

    total = 0
    xidl_total = 0
    datasets = []
    init = False
    for json_file in metadata_files:
        if not os.path.exists(json_file):
            raise MissingInputFile(json_file)

        with open(json_file, 'r') as f:
            json_obj = json.load(f)

        if not init:
            if syntool_id is not None:
                product_id = syntool_id
            else:
                product_id = json_obj['syntool_id']
            product_type = json_obj['output_type']
            product_sql = create_product(product_id, product_type)
            table_sql, table_name, table = create_product_table(product_id)
            stmt_header = 'INSERT INTO `{}` ({}) VALUES '
            stmt_header = stmt_header.format(table_name, ','.join(fields))
            sql = '{}{}'.format(product_sql, table_sql)
            exporter(sql, output_path)
            init = True

        if json_obj['begin_datetime'] == json_obj['end_datetime']:
            logger.warn('The extent of the time range for the "{}" dataset '
                        'seems to be less than one second: this is not '
                        'supported by Syntool. One second will be added to '
                        'end_datetime in order to fix this.'.format(
                        json_obj['dataset']))
            stop_dt = datetime.datetime.strptime(json_obj['end_datetime'],
                                                 TIME_FMT)
            stop_dt = stop_dt + datetime.timedelta(seconds=1)
            json_obj['end_datetime'] = stop_dt.strftime(TIME_FMT)

        if 'e_bbox' in json_obj and 'w_bbox' in json_obj:
            sql = create_dataset(table,
                                 json_obj['dataset'],
                                 json_obj['begin_datetime'],
                                 json_obj['end_datetime'],
                                 json_obj['min_zoom_level'],
                                 json_obj['max_zoom_level'],
                                 json_obj['resolutions'],
                                 json_obj['bbox_str'],
                                 json_obj['shape_str'],
                                 json_obj['w_bbox'])
            datasets.append(sql)

            extra_id = '{}_XIDLfix'.format(json_obj['dataset'])
            sql = create_dataset(table,
                                 extra_id,
                                 json_obj['begin_datetime'],
                                 json_obj['end_datetime'],
                                 json_obj['min_zoom_level'],
                                 json_obj['max_zoom_level'],
                                 json_obj['resolutions'],
                                 json_obj['bbox_str'],
                                 json_obj['shape_str'],
                                 json_obj['e_bbox'])
            datasets.append(sql)
            xidl_total += 1
        else:
            sql = create_dataset(table,
                                 json_obj['dataset'],
                                 json_obj['begin_datetime'],
                                 json_obj['end_datetime'],
                                 json_obj['min_zoom_level'],
                                 json_obj['max_zoom_level'],
                                 json_obj['resolutions'],
                                 json_obj['bbox_str'],
                                 json_obj['shape_str'],
                                 None)
            datasets.append(sql)

        # Split the insertions into several instructions because MySQL refuses
        # to execute commands once they get too big.
        if chunk_size <= len(datasets):
            logger.info('Exporting {} datasets'.format(len(datasets)))
            _sql = '{}{}{}\n'.format(stmt_header, ','.join(datasets),
                                     stmt_footer)
            exporter(_sql, output_path)
            del datasets
            datasets = []

        total += 1

    if 0 < len(datasets):
        logger.info('Exporting {} datasets'.format(len(datasets)))
        sql = '{}{}{}\n'.format(stmt_header, ','.join(datasets), stmt_footer)
        exporter(sql, output_path)

    logger.info('Exported {} datasets [+{} x-IDL]'.format(total, xidl_total))
