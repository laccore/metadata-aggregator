
'''
This script takes as input the CSDCO sqlite database and combines locations (country and state),
feature names, and PIs by project code, and then exports all that as a csv.

The exported csv can then be imported into Drupal, which updates the project and map
databases on our website.

Spreadsheet Name  Database Columns
----------------  -----------------
PROJECT ID        Expedition
LOCATION          Country,State_Province
NAMED FEATURE     Location
INVESTIGATOR      PI

TODO:
* Allow entry of a starting row, i.e., all data at and beyond which is new and only handle that data.
* Build GUI
* Use argparse
'''

import timeit
import datetime
import sqlite3
import collections
import argparse
import os

def aggregate_metadata(infile, outfile, **kwargs):
    if 'exclude_projects' in kwargs:
        exclude_projects = kwargs['exclude_projects']
    else:
        exclude_projects = []
    
    if 'debug_projects' in kwargs:
        debug_projects = kwargs['debug_projects']
    else:
        debug_projects = []



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

        c = c.replace(',','') if c else None
        s = s.replace(',','') if s else None
        f = f.replace(',','') if f else None
        p = p.split(', ') if p else None

        if e in debug_projects:
            print('expedition: {}\ncountry: {}\nstate: {}\nfeature name: {}\npis: {}\n'.format(e,c,s,f,p))
        
        c and countries[e].add(c)
        s and states[e].add(s)
        f and feature_names[e].add(f)
        if p:
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


if __name__ == '__main__':
    #     print('ERROR: invalid parameters. Exiting.')
    #     print('Usage (e.g.): python csdco-metadata-aggregator.py CSDCO.sqlite3')
    #     print('              python csdco-metadata-aggregator.py CSDCO.sqlite3 project_list_for_website.csv')
    #     exit(1)
    

    # else:
    #     outfile = 'project_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
    parser = argparse.ArgumentParser(description='Aggregate fields from the CSDCO database by Expedition Drupal for publishing.')
    parser.add_argument('db_file', type=str, help='Name of CSDCO database file.')
    parser.add_argument('-f', type=str, help='Filename for export.')
    args = parser.parse_args()

    if not os.path.isfile(args.db_file):
      print('ERROR: database file \'{}\' does not exist.\n'.format(args.db_file))
      exit(1)

    # Use filename if provided, else create using datetimestamp
    outfile = args.f if args.f else 'project_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'

    # projects to exclude from export, e.g., ocean drilling projects
    # TODO: allow passing of list via command line
    exclude_projects = ['AT15','ORCA','B0405','B0506','SBB']
        
    # print troubleshooting info
    # TODO: allow passing of list via command line    
    debug_projects = []

    start_time = timeit.default_timer()
    aggregate_metadata(args.db_file, outfile, exclude_projects=exclude_projects, debug_projects=debug_projects)
    end_time = timeit.default_timer()
    print('Completed in {0} seconds.'.format(round(end_time-start_time,2)))
