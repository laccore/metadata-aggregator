'''
This script takes as input the CSDCO sqlite database and combines locations (country and state),
feature names, and PIs by project code, and then exports all that as a csv.

The exported csv can then be imported into Drupal, which updates the project db on our website.

Export CSV Name     Database Columns
---------------     -----------------
PROJECT ID          Expedition
LOCATION            Country,State_Province
NAMED FEATURE       Location
INVESTIGATOR        PI

TODO:
* Build GUI
* Command line passing of excluded projects and debug projects
* Use csv module to cleanup export section a lot?
'''

import timeit
import datetime
import sqlite3
import collections
import argparse
import os
import csv

def aggregate_metadata(database, outfile, **kwargs):
  if 'exclude_projects' in kwargs:
    exclude_projects = kwargs['exclude_projects']
  else:
    exclude_projects = []
  
  if 'debug_projects' in kwargs:
    debug_projects = kwargs['debug_projects']
  else:
    debug_projects = []

  conn = sqlite3.connect(database)
  cur = conn.cursor()

  expeditions = set()
  countries = collections.defaultdict(set)
  states = collections.defaultdict(set)
  feature_names = collections.defaultdict(set)
  pis = collections.defaultdict(list)

  for r in cur.execute("SELECT Expedition, Country, State_Province, Location, PI FROM boreholes"):
    [e, c, s, f, p] = r
    expeditions.add(e)

    if c is not None:
      c = c.replace(',','')
      countries[e].add(c)
    if s is not None:
      s = s.replace(',','')
      states[e].add(s)
    if f is not None:
      f = f.replace(',','')
      feature_names[e].add(f)
    if p is not None:
      for pi in p.split(', '):
        if pi not in pis[e]:
          pis[e].append(pi)

    if e in debug_projects:
      print('Expedition: {}\nCountry: {}\nState: {}\nFeature Name: {}\nPIs: {}\n'.format(e,c,s,f,p))
  
  project_metadata = {}
  query_columns = ['Expedition', 'Full_Name', 'Funding', 'Technique', 'Discipline', 'Link_Title', 'Link_URL', 'Lab', 'Repository', 'Status', 'Start_Date', 'Outreach']
  query_statment = 'SELECT ' + ', '.join(query_columns) + ' FROM projects'
  for r in cur.execute(query_statment):
    project_metadata[r[0]] = r[1:]

  with open(outfile, 'w', encoding='utf-8-sig') as f:
    csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

    column_titles = ['PROJECT ID','NAME','LOCATION','NAMED FEATURE','INVESTIGATOR','FUNDING','TECHNIQUE','SCIENTIFIC DISCIPLINE','LINK TITLE','LINK URL','LAB','REPOSITORY','STATUS','START DATE','OUTREACH']
    csvwriter.writerow(column_titles)

    for e in sorted(expeditions-set(exclude_projects)):
      l = ','.join(sorted(countries[e])+sorted(states[e]))
      nf = ','.join(sorted(feature_names[e]))
      i = ','.join(pis[e])

      aggregated_line = [e, l, nf, i]

      try:
        aggregated_line += project_metadata[e]
      except KeyError:
        aggregated_line += ['']*(len(query_columns)-1)
        print('WARNING: Project {} is not in the projects table in {}.'.format(e,database))

      csvwriter.writerow(aggregated_line)

  print('{} projects found.'.format(len(expeditions-set(exclude_projects))))
  print('Aggregated data written to {}.'.format(outfile))


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Aggregate fields from the CSDCO database by Expedition for import on our Drupal website.')
  parser.add_argument('db_file', type=str, help='Name of CSDCO database file.')
  parser.add_argument('-f', type=str, help='Filename for export.')
  args = parser.parse_args()

  if not os.path.isfile(args.db_file):
    print('ERROR: database file \'{}\' does not exist.\n'.format(args.db_file))
    exit(1)

  # Use filename if provided, else create using datetimestamp
  outfile = args.f if args.f else 'project_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'

  # List of projects to exclude from export, e.g., ocean drilling projects
  exclude_projects = ['AT15','ORCA','B0405','B0506','SBB']
    
  # List of projects to print info on for troubleshooting
  debug_projects = []

  start_time = timeit.default_timer()
  aggregate_metadata(args.db_file, outfile, exclude_projects=exclude_projects, debug_projects=debug_projects)
  end_time = timeit.default_timer()
  print('Completed in {0} seconds.'.format(round(end_time-start_time,2)))
