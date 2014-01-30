import csv
import gzip
import os
import re
import sys

THREAD_COLUMN=0
SUB_COLUMN=1
APP_COLUMN=2
VER_COLUMN=3
CHAN_COLUMN=4
QUERY_COLUMN=5
COUNT_COLUMN=6
INV_COLUMN=7
TOTAL_DUR_COLUMN=8
MEDIAN_DUR_COLUMN=9

MAX_ROWS=100
# expected columns: thread_type,submission_date,app_name,app_version,app_update_channel,query,document_count,total_invocations,total_duration,median_duration

def get_key(row):
    # excluding QUERY_COLUMN
    return ",".join(row[APP_COLUMN:QUERY_COLUMN]);

db_regex1 = re.compile(r'.*\/\* ([^ ]+) \*\/');
db_regex2 = re.compile(r'^Untracked SQL for (.+)$');
def get_database(query):
    m = db_regex1.match(query)
    if m:
        return m.group(1)
    else:
        m = db_regex2.match(query)
        if m:
            return m.group(1)
    return "UNKNOWN";

def median(v, already_sorted=False):
    ls = len(v)
    if ls == 0:
        return 0
    if already_sorted:
        s = v
    else:
        s = sorted(v)
    middle = int(ls / 2)
    if ls % 2 == 1:
        return s[middle]
    else:
        return (s[middle] + s[middle-1]) / 2.0

def combine(q, rows):
    seen_keys = {}
    total_docs = 0
    q_docs = 0
    q_invocations = 0
    q_total_dur = 0
    q_median_dur = []
    for r in rows:
        #print "row:", r
        k = get_key(r)
        #print "key:", k
        try:
            if k not in seen_keys:
                total_docs += totals[k]
                seen_keys[k] = 1
        except KeyError, e:
            print "Key not found:", k, r
        q_docs += int(r[COUNT_COLUMN])
        q_invocations += int(r[INV_COLUMN])
        q_total_dur += float(r[TOTAL_DUR_COLUMN])
        #print "Found a median of", r[MEDIAN_DUR_COLUMN], "as float:", float(r[MEDIAN_DUR_COLUMN])
        q_median_dur.append(float(r[MEDIAN_DUR_COLUMN]))
    # median of medians... insane?
    q_median = median(q_median_dur)
    #print "median of", q_median_dur, "is", q_median
    return [get_database(q), round(float(q_docs) / float(total_docs) * 100, 2), q_median, q_total_dur, q] #, "--", q_invocations, q_docs, total_docs]

queries = {}
totals = {}
total_rows = 0
output_dir = sys.argv[1]
week_start = sys.argv[2]
week_end = sys.argv[3]
inputs = []

file_pattern = re.compile("^slowsql([0-9]{8}).csv.gz$")
for root, dirs, files in os.walk("."):
    for f in files:
        m = file_pattern.match(f)
        if m:
            print "found a file:", f
            d = m.group(1)
            if d >= week_start and d <= week_end:
                print "and it's good!", f
                inputs.append(os.path.join(root, f))
        else:
            print "no match file:", f

for a in inputs:
    print "processing", a
    f = gzip.open(a, 'rb')
    reader = csv.reader(f)
    headers = reader.next()
    #for i in range(len(headers)):
    #    print "field", i, "is", headers[i]
    rowcount = 0
    for row in reader:
        if len(row) > QUERY_COLUMN:
            if row[0] == "TOTAL":
                # sum up the submission_dates.
                total_key = get_key(row)
                if total_key not in totals:
                    totals[total_key] = 0
                totals[total_key] += int(row[COUNT_COLUMN])
            else:
                q = row[QUERY_COLUMN]
                if q not in queries:
                    queries[q] = []
                queries[q].append(row)
        #else:
        #    print "not enough columns:", row
        rowcount += 1
    print "Found", rowcount, "rows"
    total_rows += rowcount
    f.close()

print "overall, found", total_rows, "rows, with", len(queries.keys()), "unique queries"

combined = []
for q, rows in queries.iteritems():
    print "Combining", len(rows), "items for", q
    combined.append(combine(q, rows))

for stub, column in [["frequency", 1], ["median_duration", 2], ["total_duration", 3]]:
    filename = "weekly_{0}_{1}-{2}.csv".format(stub, week_start, week_end)
    print "Generating", filename
    outfile = open(filename, "w")
    writer = csv.writer(outfile)
    rowcount = 0
    #writer.writerow(["db_name", "frequency", "median_duration", "total_duration", "query"])
    #db_name, frequency, median_duration, total_duration, query
    for row in sorted(combined, key=lambda r: r[column], reverse=True):
        writer.writerow(row)
        rowcount += 1
        if rowcount >= MAX_ROWS:
            break
    outfile.close()

# Output columns:
#db_name, frequency, median_duration, total_duration, query
