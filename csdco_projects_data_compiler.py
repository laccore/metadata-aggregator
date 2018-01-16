
'''
Field           Column Number
-------------   -------------
Expedition      7
Location        5
Country         17
State_Province  18
PI              20


Spreadsheet Name	Excel Name
----------------	-----------------
PROJECT ID		    Expedition
LOCATION			Country,State_Province
NAMED FEATURE	    Location
INVESTIGATOR		PI
'''

import sys
import timeit
import csv
import datetime

start_time = timeit.default_timer()

debug_projects = []

infile = sys.argv[1]
if len(sys.argv) > 2:
    outfile = sys.argv[2]
else:
    outfile = 'project_data_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.csv'

with open(infile, 'r', encoding='utf-8-sig') as f:
	rawdata = f.read().splitlines()

data = []

# The Excel file often exports a huge amount of empty rows of varying number of columns. This ignores those.
for r in rawdata:
    if r.replace(',','') != '':
        data += [r]

data = data[1:]     # ignore hearder
rdata = []

for r in csv.reader(data, quotechar='"', delimiter=','):
    rdata += [r[:32]]

expeditions = set()
countries = {}
states = {}
featureNames = {}
pis = {}


for r in rdata:
    e = r[7]
    expeditions.add(e)

    c = r[17]
    s = r[18]
    f = r[5].replace(',','')
    p = r[20].split(', ')

    if e in debug_projects:
        print('expedition: {}\ncountry: {}\nstate: {}\nfeatures: {}\npis: {}\n'.format(e,c,s,f,p))
    
    if e not in countries:
        countries[e] = [c]
        states[e] = [s]
        featureNames[e] = [f]
        pis[e] = p
    else:
        countries[e] += [c]
        states[e] += [s]
        featureNames[e] += [f]
        pis[e] += p


# Cleanup and stuff
empty = {'', 'n/a', 'N/A'}
for c in countries:
    countries[c] = list(set(countries[c])-empty)
for s in states:
    states[s] = list(set(states[s])-empty)
for f in featureNames:
    featureNames[f] = list(set(featureNames[f])-empty)
for p in pis:
    pis[p] = list(set(pis[p])-empty)


with open(outfile, 'w', encoding='utf-8-sig') as f:
    f.write('\"PROJECT ID\",\"LOCATION\",\"NAMED FEATURE\",\"INVESTIGATOR\"\n')

    for e in sorted(expeditions):
        l = ','.join(sorted(countries[e])+sorted(states[e]))
        nf = ','.join(sorted(featureNames[e]))
        i = ','.join(pis[e])

        f.write('\"' + e + '\",\"' + l + '\",\"' + nf + '\",\"' + i + '\"\n')

print('{0} projects found.'.format(len(expeditions)))
print('Aggregated data written to {0}.'.format(outfile))

end_time = timeit.default_timer()
print('Completed in {0} seconds.'.format(round(end_time-start_time,2)))
