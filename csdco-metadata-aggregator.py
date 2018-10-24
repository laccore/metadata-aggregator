
'''
This script takes as input a CSV export of our LacCore_Holes_YYYYMMDD.xlsx file and 
combines locations (country and state), feature names, and PIs by project code, with
some general data cleanup along the way. It then exports all that as a csv.

The exported csv can then be imported into Drupal, which updates the project and map
databases on our website.

Field           Column Number
-------------   -------------
Expedition      7
Location        5
Country         17
State_Province  18
PI              20


Spreadsheet Name  Excel Name
----------------  -----------------
PROJECT ID        Expedition
LOCATION          Country,State_Province
NAMED FEATURE     Location
INVESTIGATOR      PI

TODO:
* Allow entry of a starting row, i.e., all data at and beyond which is new and only handle that data.
* Build GUI
* Use argparse
* use numpy to handle Excel?
'''

import sys
import timeit
import datetime
import sqlite3
import collections

start_time = timeit.default_timer()


def aggregate_metadata(infile, outfile, **kwargs):
    if 'exclude_projects' in kwargs:
        exclude_projects = kwargs['exclude_projects']
    else:
        exclude_projects = []
    
    if 'debug_projects' in kwargs:
        debug_projects = kwargs['debug_projects']
    else:
        debug_projects = []
    
    # with open(infile, 'r', encoding='utf-8-sig') as f:
    #     rawdata = f.read().splitlines()

    # # The Excel file often exports a huge amount of empty rows of varying number of columns. This ignores those.
    # data = [r for r in rawdata[1:] if r.replace(',','') != '']

    # rdata = [r[:32] for r in csv.reader(data, quotechar='"', delimiter=',')]

    conn = sqlite3.connect(infile)
    c = conn.cursor()

    expeditions = set()
    countries = collections.defaultdict(set)
    states = collections.defaultdict(set)
    feature_names = collections.defaultdict(set)
    pis = collections.defaultdict(list)

    for r in c.execute("SELECT Expedition, Country, State_Province, Location, PI FROM boreholes"):
        [e, c, s, f, p] = r
        expeditions.add(e)

        c = c.replace(',','') if c else ''
        s = s.replace(',','') if s else ''
        f = f.replace(',','') if f else ''
        p = p.split(', ') if p else ''

        if e in debug_projects:
            print('expedition: {}\ncountry: {}\nstate: {}\nfeature name: {}\npis: {}\n'.format(e,c,s,f,p))
        
        countries[e].add(c)
        states[e].add(s)
        feature_names[e].add(f)
        for pi in p:
            if pi not in pis[e]:
                pis[e].append(pi)


    with open(outfile, 'w', encoding='utf-8-sig') as f:
        f.write('\"PROJECT ID\",\"LOCATION\",\"NAMED FEATURE\",\"INVESTIGATOR\"\n')

        for e in sorted(expeditions-set(exclude_projects)):
            l = ','.join(sorted(countries[e])+sorted(states[e]))
            nf = ','.join(sorted(feature_names[e]))
            i = ','.join(pis[e])

            f.write('\"' + e + '\",\"' + l + '\",\"' + nf + '\",\"' + i + '\"\n')


    print('{0} projects found.'.format(len(expeditions-set(exclude_projects))))
    print('Aggregated data written to {0}.'.format(outfile))

    end_time = timeit.default_timer()
    print('Completed in {0} seconds.'.format(round(end_time-start_time,2)))

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print('ERROR: invalid parameters. Exiting.')
        print('Usage (e.g.): python csdco-metadata-aggregator.py CSDCO.sqlite3')
        print('              python csdco-metadata-aggregator.py CSDCO.sqlite3 project_list_for_website.csv')
        exit(1)
    
    infile = sys.argv[1]

    if len(sys.argv) > 2:
        outfile = sys.argv[2]
    else:
        outfile = 'project_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'

    # projects to exclude from export, e.g., ocean drilling projects
    # TODO: allow passing of list via command line
    exclude_projects = ['AT15','ORCA','B0405','B0506','SBB']
        
    # print troubleshooting info
    # TODO: allow passing of list via command line    
    debug_projects = []

    aggregate_metadata(infile, outfile, exclude_projects=exclude_projects, debug_projects=debug_projects)
