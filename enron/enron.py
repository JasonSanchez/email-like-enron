import glob
import hashlib
import os
import zipfile
from functools import partial

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import _libpst

if __name__ == "__main__":
    conf = SparkConf().setAppName("ProcessEnron")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    p = glob.glob('/enron/edrm-enron-v2/*_pst.zip')

    pst_files = sc.parallelize(p, 15)

    temp_path = '/mnt/'

    # These are phrases that signifiy everything
    # following in a email is junk
    msg_noise = ['***********\r\nEDRM Enron Email',
                 'Forwarded by',
                 'Original Message',
                 'EOL Deals']

    subj_noise = ['Real Time Deals']

    # open the zipfile
    def extract(zf, outpath):
        with open(zf, 'rb') as fileobj:
            z = zipfile.ZipFile(fileobj)
            z.extractall(outpath)
            z.close()
        fl = z.filelist[0].filename
        return outpath + fl

    # do something with the file.
    def process_pst(psts):
        # do something with the file
        m = hashlib.md5()
        pst = _libpst.pst(psts, "")
        topf = pst.pst_getTopOfFolders()
        while topf:
            row = {"title": 'Enron',
                   "author": None,
                   "contents": None,
                   "part": 0,
                   "hash": None}

            item = pst.pst_parse_item(topf, None)
            if item and item.type == 1 and item.email:
                # em = item.email
                subj = item.subject
                body = item.body

                if any(x in subj.str for x in subj_noise):
                    pst.pst_freeItem(item)
                    topf = pst.pst_getNextDptr(topf)
                    continue

                if body.str:
                    body = body.str

                    for noise in msg_noise:
                        if noise in body:
                            body = body.split(noise, 1)[0]

                    # Strip out all the extra whitespace
                    body = ' '.join(body.split())

                    # strip any residual stuff from the
                    # removing the forwards.
                    body = body.rstrip('-')

                    # lets skip if short
                    # greater than 50 characters is nice
                    if len(body) > 50:
                        m.update(body)

                        row['contents'] = body
                        row['hash'] = m.hexdigest()
                        row['author'] = item.email.sender_address.str

                        yield row

            pst.pst_freeItem(item)
            topf = pst.pst_getNextDptr(topf)
        os.remove(psts)

    out = pst_files.map(
        partial(extract, outpath=temp_path)).flatMap(process_pst).toDF().save(
            "s3n://datasets-396316040607/enron_data/",
            "com.databricks.spark.avro")

    sc.stop()
