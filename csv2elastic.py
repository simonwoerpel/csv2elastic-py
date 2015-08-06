import os
import codecs
import argparse
import csv
import simplejson as json
import uuid
from datetime import datetime
from elasticsearch import Elasticsearch


def ask_to_continue(prompt):
    while True:
        if not str(input(prompt+'\npress [y] to continue or ctrl-c to abort\n')) == 'y':
            continue
        else:
            break
    return True


def ask_for_bool(prompt):
    while True:
        bool_input = str(input(prompt+' [y/n]\n'))
        if bool_input not in ('y','n'):
            continue
        else:
            break
    if bool_input == 'y':
        return True
    else:
        return False


def reassign_keys(header):
    # TODO not working yet
    while True:
        if str(input('are these keys ok? [y/n]\n')) == 'y':
            new_keys = header.split(';')
            break
        else:
            while True:
                new_keys = input('specify comma seperated list of new keys:\n')
                if not len(new_keys.split(',')) == len(header.split(';')):
                    print('number of new keys is not matching number of old keys')
                    continue
                else:
                    print('new keys OK')
                    break
    return new_keys


def convert_record(r, date_fields=[], decimal_fields=[], replacing={}, geo_handling=None, extra_data=None):
    """
    converts r (already a dict from csv.DictReader) into better dict for elasticsearch
    currently only date formatting, decimal formatting, replacing of values
    """
    d = {}
    clean_d = {k: v.strip() for k, v in r.items()}
    for k in clean_d.keys():
        if clean_d[k] and k in date_fields:
            try:
                d[k] = datetime.strptime(clean_d[k], date_strformat).date().isoformat()
            except ValueError as e:
                raise(e)
                d[k] = clean_d[k]
        elif k in decimal_fields:
            try:
                d[k] = float(clean_d[k])
            except ValueError:
                try:
                    # assuming numbers have , as decimal delimiter and . as thousands seperator!
                    # FIXME
                    d[k] = float(clean_d[k].replace('.', '').replace(',', '.'))
                except ValueError:
                    d[k] = clean_d[k]
        elif k in [f for f in replacing]:
            try:
                d[k] = replacing[k][clean_d[k]]
            except KeyError:
                d[k] = clean_d[k]
        elif geo_handling and (k in (geo_handling['latfield'], geo_handling['lonfield'])):
            if geo_handling['combinedfield'] not in d:
                d[geo_handling['combinedfield']] = {}
            if k == geo_handling['latfield']:
            # for elasticsearch mapping of geo_type, lat & lon must be a number
                try:
                    lat = float(clean_d[k].replace(',', '.'))
                except ValueError:
                    lat = 0
                d[geo_handling['combinedfield']]['lat'] = lat
            else:
                try:
                    lon = float(clean_d[k].replace(',', '.'))
                except ValueError:
                    lon = 0
                d[geo_handling['combinedfield']]['lon'] = lon
        else:
            d[k] = clean_d[k]
    if extra_data:
        for k in extra_data.keys():
            if k not in list(d.keys()):
                d[k] = extra_data[k]
    return d


# ARGS

parser = argparse.ArgumentParser()
parser.add_argument('inputfile', type=str, help='the input file, must be .csv with ONLY 1 header row and delimited by ;')
parser.add_argument('configfile', type=str, help='the config file, must be .json dict', nargs='?', default=None)
args = parser.parse_args()
input_fp = args.inputfile
config = json.load(open(args.configfile))


# BEGIN


print('processing '+input_fp)
if config:
    print('with config for %s' % ', '.join([k for k in config]))

if not os.path.isfile(input_fp):
    raise Exception(input_fp+' cannot be found')


print('fetching keys...')
try:
    with open(input_fp) as f:
        header = f.readline()
except UnicodeDecodeError:
    with codecs.open(input_fp, 'r', 'iso-8859-1') as f:
        header = f.readline()

print('these are the keys:\n')
print('\n'.join(header.split(';')))
ask_to_continue('are these keys ok?')
# new_keys = reassign_keys(header)
# print('...using these keys:\n\n'+'\n'.join(new_keys))


print('loading file records into memory...')


try:
    with open(input_fp) as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = []
        i = 0
        for row in reader:
            rows.append(row)
            i += 1
            if i == 1000:
                print('.', end='')
                i = 0
except UnicodeDecodeError:
    with codecs.open(input_fp, 'r', 'iso-8859-1') as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = []
        i = 0
        for row in reader:
            rows.append(row)
            i += 1
            if i == 1000:
                print('.', end='')
                i = 0


print('\nOK. '+input_fp+' has '+str(len(rows))+' records\n')
print('this would be a sample json record:\n')
print(json.dumps(convert_record(rows[0])))


# DATE FORMATTING

if 'date_formatting' not in config:
    change = ask_for_bool('want to change date formatting?')

    if change:
        print('changing date formatting')
        while True:
            date_fields = [f.strip() for f in input('enter comma seperated list of field keys that should converted into isoformat:\n').split(',')]
            if not set(date_fields) < set(list(rows[0].keys())):
                print('these keys are not in %s' % ', '.join(list(rows[0].keys())))
                continue
            else:
                date_strformat = str(input('enter python date input format (e.g. %d.%m.%Y)'))
                break
        print('generating new sample record...')
        d = convert_record(rows[0], date_fields)
        print('...this is how it looks like now:')
        print(json.dumps(d))
    else:
        date_fields = []
        print('ok, nothing to change.')
else:
    date_fields = config['date_formatting']['fields']
    date_strformat = config['date_formatting']['strformat']
    print('changing date formating (input: %s) for fields %s...' % (date_strformat, date_fields))
    print('generating new sample record...')
    d = convert_record(rows[0], date_fields)
    print('...this is how it looks like now:')
    print(json.dumps(d))


# DECIMAL

if 'decimal_fields' not in config:
    change = ask_for_bool('want to change decimal formatting?')

    if change:
        print('changing decimal formatting')
        while True:
            decimal_fields = [f.strip() for f in input('enter comma seperated list of field keys that should converted into decimal:\n').split(',')]
            if not set(decimal_fields) < set(list(rows[0].keys())):
                print('these keys are not in %s' % ', '.join(list(rows[0].keys())))
                continue
            else:
                break
        print('generating new sample record...')
        d = convert_record(rows[0], date_fields, decimal_fields)
        print('...this is how it looks like now:')
        print(json.dumps(d))
    else:
        decimal_fields = []
        print('ok, nothing to change.')
else:
    decimal_fields = config['decimal_fields']
    print('changing decimal formating for fields %s...' % decimal_fields)
    print('generating new sample record...')
    d = convert_record(rows[0], date_fields, decimal_fields)
    print('...this is how it looks like now:')
    print(json.dumps(d))


# REPLACING

if 'replacing' in config:
    print('replacing with this scheme:')
    replacing = config['replacing']
    print('\n%s' % replacing)
    d = convert_record(rows[0], date_fields, decimal_fields, replacing)
    print('...this is how it looks like now:')
    print(json.dumps(d))
    ask_to_continue('Cool?')
else:
    replacing = {}

# GEODATA
if 'geo' in config:
    print('combining geodata with this scheme:')
    geohandling = config['geo']
    print('\n%s' % geohandling)
    d = convert_record(rows[0], date_fields, decimal_fields, replacing, geohandling)
    print('...this is how it looks like now:')
    print(json.dumps(d))
    ask_to_continue('Cool?')
else:
    geohandling = None


# EXTRA DATA

if 'extra_data' not in config:
    add_data = ask_for_bool('do you want to add some data to each record?')

    if add_data:
        while True:
            append_dict = str(input('insert extra data as valid json dict:\n'))
            try:
                extra_data = json.loads(append_dict)
                break
            except Exception as e:
                print(e)
                continue
    else:
        extra_data = {}
else:
    print('using extra data from config...')
    extra_data = config['extra_data']
    print(extra_data)


print('generating json sample with extra data')
print(json.dumps(convert_record(rows[0], date_fields, decimal_fields, replacing, geohandling, extra_data)))

ask_to_continue('looks good?')

# id_field = get_id_field(rows[0])

# print('using %s as id field... OK' % id_field)

# print('performing some tests for first upsert_package...')
print('performing FULL TEST...')
# FULL TEST
docs = []
i = 1

for row in rows:
    try:
        docs.append(convert_record(row, date_fields, decimal_fields, replacing, geohandling, extra_data))
        print('tested: %s of %s' % (str(i), str(len(rows))))
    except Exception as e:
        print('ERROR:\n')
        print(e)
        print('error occured in row %s' % i)
        print(row)
        raise
    i += 1


print('test about %s records successfull!' % len(docs))


print('\nthis kind of data will be imported:\n')
print(docs[0:3])


ask_to_continue('start importing?')


# ELASTIC

print('connecting to elasticsearch...')

es = Elasticsearch()

try:
    print(es.info())
except Exception as e:
    print(e)
    raise

print('...OK')

# ELASTIC CONFIG
try:
    es_index = config['elasticsearch']['index']
except KeyError:
    es_index = None

try:
    doc_type = config['elasticsearch']['doc_type']
except KeyError:
    doc_type = None

if not es_index:
    while True:
        index = str(input('insert index name:\n'))
        if not index.strip():
            continue
        else:
            break

if not doc_type:
    while True:
        doc_type = str(input('insert index name:\n'))
        if not doc_type.strip():
            continue
        else:
            break

# START IMPORT

print('importing '+str(len(docs))+' records into elasticsearch...')

i = 1

for doc in docs:
    try:
        es.index(index=es_index, doc_type=doc_type, id=uuid.uuid4(), body=doc)
        print('imported: %s of %s' % (str(i), str(len(docs))))
    except Exception as e:
        print('error while elasticsearch insert:\n')
        print(e)
        print('doc row:' + str(i))
    i += 1

print('END: successfully imported %s records into elasticsearch!' % str(i))
