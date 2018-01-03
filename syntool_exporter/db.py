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

import logging
import sqlalchemy
import sqlalchemy.dialects.mysql.base
import sqlalchemy.ext.declarative
from sqlalchemy.schema import CreateTable
from sqlalchemy import func
from sqlalchemy.types import UserDefinedType

logger = logging.getLogger(__name__)

Base = sqlalchemy.ext.declarative.declarative_base()


# Database Mappings
# -----------------------------------------------------------------------------
class Geometry(UserDefinedType):
    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        value = func.GeomFromText(bindvalue, type_=self)
        return value

    def column_expression(self, col):
        return func.AsText(col, type_=self)


Base = sqlalchemy.ext.declarative.declarative_base()


class Product(Base):
    """ """
    __tablename__ = 'products'
    product_id = sqlalchemy.Column(sqlalchemy.types.VARCHAR(255),
                                   primary_key=True)
    shortname = sqlalchemy.Column(sqlalchemy.types.VARCHAR(255),
                                  nullable=False)
    type = sqlalchemy.Column(sqlalchemy.types.VARCHAR(255), nullable=False)

    def __init__(self):
        """ """
        self.type = 'ZXY'


class Dataset():
    """ """
    dataset_name = sqlalchemy.Column(sqlalchemy.types.VARCHAR(255),
                                     nullable=False, primary_key=True)
    relative_path = sqlalchemy.Column(sqlalchemy.types.VARCHAR(255),
                                      nullable=False)
    begin_datetime = sqlalchemy.Column(sqlalchemy.types.DATETIME,
                                       nullable=False)
    end_datetime = sqlalchemy.Column(sqlalchemy.types.DATETIME,
                                     nullable=False)
    min_zoom_level = sqlalchemy.Column(sqlalchemy.types.INTEGER,
                                       nullable=False)
    max_zoom_level = sqlalchemy.Column(sqlalchemy.types.INTEGER,
                                       nullable=False)
    resolutions = sqlalchemy.Column(sqlalchemy.types.TEXT, nullable=False)
    bbox_text = sqlalchemy.Column(sqlalchemy.types.TEXT, nullable=False)
    bbox_geometry = sqlalchemy.Column(Geometry, nullable=True)
    shape_text = sqlalchemy.Column(sqlalchemy.types.TEXT, nullable=False)
    shape_geometry = sqlalchemy.Column(Geometry, nullable=False)


# Serialization helpers
# -----------------------------------------------------------------------------
class StringLiteral(sqlalchemy.types.String):
    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int):
                return str(value)
            if not isinstance(value, str):
                value = str(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result
        return process


class LiteralDialect(sqlalchemy.dialects.mysql.base.MySQLDialect):
    colspecs = {
        sqlalchemy.sql.sqltypes.String: StringLiteral,
        sqlalchemy.sql.sqltypes.DateTime: StringLiteral,
        sqlalchemy.sql.sqltypes.NullType: StringLiteral,
    }


def get_product_table_name(product_id):
    return 'product_{}'.format(product_id.replace(' ', '_'))


def get_product_table(product_id):
    fixed_product_id = product_id.replace(' ', '_')
    result = type('Dataset_{}'.format(fixed_product_id),
                  (Base, Dataset),
                  {'__tablename__': 'product_{}'.format(fixed_product_id)})
    return result


def upsert(fields):
    """ """
    stmt_footer = ' ON DUPLICATE KEY UPDATE {};'.format(
        ','.join(map(lambda x: '{}=VALUES({})'.format(x, x), fields)))
    return stmt_footer


def create_missing_table(tbl):
    """"""
    stmt = CreateTable(tbl.__table__)
    stmt = stmt.compile(dialect=LiteralDialect())
    stmt_str = stmt.__str__()
    stmt_end = stmt_str.rfind(')')
    stmt_str = '{});\n'.format(stmt_str[:stmt_end])
    sql = stmt_str.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    return sql


def create_product(product_id, product_type):
    """Build SQL statement for inserting a product in the 'products' table."""
    create_sql = create_missing_table(Product)
    ins = Product.__table__.insert()
    stmt = ins.values(product_id=product_id,
                      shortname=product_id,
                      type=product_type)
    stmt_str = stmt.compile(dialect=LiteralDialect(),
                            compile_kwargs={"literal_binds": True}).__str__()
    stmt_footer = upsert(('shortname', 'type'))
    sql = '{}{} {};\n'.format(create_sql, stmt_str, stmt_footer)
    return sql


def create_product_table(product_id):
    """Build SQL statement for creating the table that will hold the datasets
    information."""
    table_name = get_product_table_name(product_id)
    table = get_product_table(product_id)
    sql = create_missing_table(table)
    return sql, table_name, table


def create_dataset(table, name, start, stop, min_zoom, max_zoom,
                   resolutions, bbox_str, shape_str, shape_extra_wkt=None):
    """ """
    if shape_extra_wkt is None:
        shape_extra_wkt = shape_str
    ins = table.__table__.insert()
    stmt = ins.values(dataset_name=name,
                      relative_path='',
                      begin_datetime=start,
                      end_datetime=stop,
                      min_zoom_level=min_zoom,
                      max_zoom_level=max_zoom,
                      resolutions=','.join(map(str, resolutions)),
                      bbox_text=bbox_str,
                      bbox_geometry=None,
                      shape_text=shape_str,
                      shape_geometry=shape_extra_wkt)
    compiled_stmt = stmt.compile(dialect=LiteralDialect(),
                                 compile_kwargs={"literal_binds": True})
    stmt_str = compiled_stmt.__str__()
    sql = stmt_str % ('NULL',
                      '\'{}\''.format(compiled_stmt.params['shape_geometry']))
    sql = sql.replace('GeomFromText(NULL)', 'NULL')
    sql = sql[sql.find('VALUES')+7:]
    return sql
