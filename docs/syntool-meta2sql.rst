syntool-meta2sql
================
The syntool-meta2sql command reads the metadata.json files produced by
syntool-ingestor to obtain information about the ingested granules and to
generate the SQL statements used to register the granules in the Syntool
database.

::
    syntool-meta2sql [--init-only] [--chunk_size CHUNK_SIZE]
                     [--product_id PRODUCT_ID] OUTPUT_PATH -- INPUT


+--------------+---------+------------------------------------------------------------------------+
| Parameter    | Format  | Description                                                            |
+--------------+---------+------------------------------------------------------------------------+
| OUTPUT_PATH  | path    | Path of the SQL file where results will be written to. If “-” is used, |
|              |         | then the SQL statements will be printed to standard output instead.    |
+--------------+---------+------------------------------------------------------------------------+
| --chunk_size | integer | Max number of datasets exported with one INSERT SQL statement.         |
+--------------+---------+------------------------------------------------------------------------+
| --product_id | string  | Override the identifier of the product read in the input files.        |
+--------------+---------+------------------------------------------------------------------------+
| --init-only  |         | Produce the SQL statements to initialize the tables of an empty SQL    |
|              |         | database.                                                              |
+--------------+---------+------------------------------------------------------------------------+

Examples
--------

Print SQL statements generated from metadata files listed by the find command,
with INSERT statements limited to 100 entries:

::
    find ingested_dir -mindepth 2 -maxdepth 2 -name "metadata.json" \
      | syntool-meta2sql --chunk_size=100 - --


Save SQL statements generated from a fixed list of metadata files, with an
override of the product identifier:

::
    syntool-meta2sql --product_id=3857_dummy /tmp/dummy.sql -- \
      dataset1/metadata.json dataset2/metadata.json

Save SQL statements required to initialize the MySQL database:

::
    syntool-meta2sql --init-only /tmp/syntool_init.sql
