#!/usr/bin/env python
import csv
import hashlib

import avro.schema
from avro.datafile import DataFileWriter
from avro.datafile import DataFileReader

from avro.io import DatumWriter
from avro.io import DatumReader


# Read the schema file. In avro, schemas are sepearte from data.
# Take a look at it, I had to change some of the names
# to avoid collisions with what I think are avro restricted
# keys.
# I got the emails from archive.org
# https://archive.org/stream/hillary-clinton-emails-august-31-release/hillary-clinton-emails-august-31-release_djvu.txt
# These emails were released as actual printed files by Hillary, so it's a huge
# pain in the ass to parse them. Everything out there has either already been
# semi processed into databases in ways which aren't very useful for us or they
# have been processed into single giant txt documents.
# We are going to deal with the single giant text document.

SCHEMA = "emails.avsc"
OUT_AVRO = "data.avro"
IN_EMAILS = "Emails.csv"
TITLE = "hillary"

m = hashlib.md5()

schema = avro.schema.parse(open(SCHEMA).read())


with DataFileWriter(open(OUT_AVRO, "w"), DatumWriter(), schema) as writer:
    with open(IN_EMAILS) as e:
        reader = csv.DictReader(e)
        for row in reader:
            txt = row['RawText']
            m.update(txt)
            # Avro can only write unicode objects which is why we are turning
            # all the strings into unicode.
            writer.append({"title": TITLE.decode('ascii', 'replace'),
                           "author": row['ExtractedFrom'].decode('ascii',
                                                                 'replace'),
                           "contents": txt.decode('ascii', 'replace'),
                           "part": 0,  # Emails are always part 0
                           "hash": m.hexdigest().decode('ascii', 'replace')})

# To read back

reader = DataFileReader(open(OUT_AVRO, "r"), DatumReader())
for email in reader:
    print email
reader.close()
