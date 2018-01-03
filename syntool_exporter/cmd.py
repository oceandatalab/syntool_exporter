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

import sys
import logging
import argparse
import fileinput
from syntool_exporter import init_mysql, convert_to_mysql, MissingInputFile

logger = logging.getLogger()
handler = logging.StreamHandler(stream=sys.stderr)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def parse_args():
    """"""
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', type=str,
                        help='Output path. Use "-" for stdout')
    parser.add_argument('--init-only', action='store_true', default=False,
                        help='Only generate SQL tables schema')
    parser.add_argument('--chunk_size', type=int, required=False, default=3000,
                        help='Max number of datasets exported with one INSERT')
    parser.add_argument('--product_id', type=str, required=False, default=None,
                        help='Override the identifier of the product')
    args, unknown = parser.parse_known_args()
    return args, unknown


def stdout_exporter(stmt, path):
    """Print statements to stdout."""
    print(stmt)


def file_exporter(stmt, path):
    """Write statements into a file."""
    with open(path, 'a') as f:
        f.write(stmt)


def to_sql():
    """"""
    args, unknown = parse_args()

    _exporter = None
    if '-' == args.output_path:
        _exporter = stdout_exporter
    else:
        logger.info('Exporting to {}'.format(args.output_path))
        # Truncate output file
        with open(args.output_path, 'w') as f:
            f.write('')
        _exporter = file_exporter

    if args.init_only:
        init_mysql(_exporter, args.output_path)
    else:
        if 0 < len(unknown):
            metadata_files = unknown
        else:
            metadata_files = fileinput.input(unknown)

        metadata_files = [x.strip() for x in metadata_files]
        try:
            convert_to_mysql(_exporter, args.output_path, metadata_files,
                             args.chunk_size, args.product_id)
        except MissingInputFile:
            _, e, _ = sys.exc_info()
            msg = 'The following file cannot be found: {}'.format(e.path)
            logger.error(msg)
            sys.exit(1)

if '__main__' == __name__:
    to_sql()
