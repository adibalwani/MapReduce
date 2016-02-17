# Author: Rachit Puri

from itertools import groupby

for key, rows in groupby(open("finaloutput"),
                         lambda row: row[0:2]):
    with open("splitfiles/%s.txt" % key, "w") as output:
        for row in rows:
            output.write("".join(row))
