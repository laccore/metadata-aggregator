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
'''

import timeit
import datetime
import sqlite3
import collections
import os
import csv
from gooey import Gooey, GooeyParser

def aggregate_metadata(database, outfile, **kwargs):
  exclude_projects = kwargs['exclude_projects'] if 'exclude_projects' in kwargs else []
  debug_projects = kwargs['debug_projects'] if 'debug_projects' in kwargs else []

  # Create empty dicts and lists for aggregation
  expeditions = set()
  countries = collections.defaultdict(set)
  states = collections.defaultdict(set)
  feature_names = collections.defaultdict(set)
  pis = collections.defaultdict(list)

  # Set up database connection in uri mode so as to open as read-only
  conn = sqlite3.connect('file:' + database + '?mode=ro', uri=True)
  cur = conn.cursor()

  # Build dictionaries (key = expedition code) with all countries, states/provinces, location names, and PIs
  # There is a many-to-one relationship between boreholes and projects, so need to aggregate data across records
  # Most will be sorted alphabetically, but order matters in academia, so it's preserved for PIs 
  for r in cur.execute("SELECT Expedition, Country, State_Province, Location, PI FROM boreholes"):
    [e, c, s, l, p] = r
    expeditions.add(e)

    if c is not None:
      c = c.replace(',','')
      countries[e].add(c)
    if s is not None:
      s = s.replace(',','')
      states[e].add(s)
    if l is not None:
      l = l.replace(',','')
      feature_names[e].add(l)
    if p is not None:
      for pi in p.split(', '):
        if pi not in pis[e]:
          pis[e].append(pi)

    if e in debug_projects:
      print(f'Expedition:\t{e}\nCountry:\t{c}\nState:\t\t{s}\nFeature Name:\t{l}\nPIs:\t\t{p}\n', flush=True)
  
  
  # Empty dict for project metadata. No additional post-processing needed, so comparatively simply setup
  project_metadata = {}

  # Build dictionary (key = expedition code) for all other associated data
  query_columns = ['Expedition', 'Full_Name', 'Funding', 'Technique', 'Discipline', 
                   'Link_Title', 'Link_URL', 'Lab', 'Repository', 'Status', 
                   'Start_Date', 'Outreach', 'Investigators']
  query_statment = 'SELECT ' + ', '.join(query_columns) + ' FROM projects'
  for r in cur.execute(query_statment):
    project_metadata[r[0]] = r[1:]
  
  no_borehole_info = set(project_metadata.keys())-expeditions

  with open(outfile, 'w', encoding='utf-8-sig') as f:
    csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    column_titles = ['PROJECT ID', 'NAME', 'LOCATION', 'NAMED FEATURE', 'INVESTIGATOR',
                     'FUNDING', 'TECHNIQUE', 'SCIENTIFIC DISCIPLINE', 'LINK TITLE',
                     'LINK URL', 'LAB', 'REPOSITORY', 'STATUS', 'START DATE', 'OUTREACH']
    csvwriter.writerow(column_titles)

    for e in sorted(expeditions-set(exclude_projects)):
      l = ','.join(sorted(countries[e])+sorted(states[e]))
      nf = ','.join(sorted(feature_names[e]))
      p = ','.join(pis[e])

      aggregated_line = [e, l, nf, p]

      # Can't guarantee all projects will be in the projects table, so catch the KeyError if they aren't
      # Also do some ugly reordering of data because a specific column order is needed for Drupal
      try:
        aggregated_line = [aggregated_line[0]] + [project_metadata[e][0]] + aggregated_line[1:] + list(project_metadata[e][1:-1])
      except KeyError:
        # If projects in the borehole table are not in projects table, fill columns with empty strings
        aggregated_line = [aggregated_line[0]] + [''] + aggregated_line[1:] + ['']*(len(query_columns)-2)
        print(f'WARNING: Project {e} is not in the projects table in {database}.', flush=True)

      if e in debug_projects:
        print(f'Full, aggregated data for writing:\n{aggregated_line}\n', flush=True)

      csvwriter.writerow(aggregated_line)
    
    # Include projects that don't have information from the borehole table. In this case, leave
    # location and named feature fields empty, but pull PI info from projects table also.
    for e in sorted(list(no_borehole_info)):
      aggregated_line = [e] + [project_metadata[e][0]] + ['']*2 + [project_metadata[e][-1]] + list(project_metadata[e][1:-1])
      csvwriter.writerow(aggregated_line)

  conn.close()

  print(f'{len(expeditions-set(exclude_projects))} projects found.', flush=True)
  print(f'Aggregated data written to {outfile}.', flush=True)


def export_project_location_data(database, outfile, **kwargs):
  exclude_projects = kwargs['exclude_projects'] if 'exclude_projects' in kwargs else []
  debug_projects = kwargs['debug_projects'] if 'debug_projects' in kwargs else []

  conn = sqlite3.connect('file:' + database + '?mode=ro', uri=True)
  cur = conn.cursor()

  with open(outfile, 'w', encoding='utf-8-sig') as f:
    csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    column_titles = ['PROJECT ID','LOCATION', 'ORIGINAL ID', 'HOLE ID', 'DATE', 
                     'WATER DEPTH', 'COUNTRY', 'STATE', 'COUNTY', 'LATITUDE', 
                     'LONGITUDE', 'ELEVATION', 'SAMPLE TYPE', 'DEPTH TOP', 'DEPTH BOTTOM']
    csvwriter.writerow(column_titles)

    query_columns = ['Expedition', 'Location', 'Original_ID', 'Hole_ID', 'Date', 
                     'Water_Depth', 'Country', 'State_Province', 'County_Region', 
                     'Lat', 'Long', 'Elevation', 'Sample_Type', 'mblf_T', 'mblf_B']
    query_statment = 'SELECT ' + ', '.join(query_columns) + ' FROM boreholes ORDER BY Expedition, Location, Original_ID'
  
    for r in cur.execute(query_statment):
      if r[0] not in exclude_projects:
        csvwriter.writerow(r)
      if r[0] in debug_projects:
        print(f'Metadata information for {r[0]}:\n{r}', flush=True)
    
  conn.close()
  
  print(f'Project location data written to {outfile}.', flush=True)


@Gooey(program_name='CSDCO Metadata Aggregator')
def main():
  parser = GooeyParser(description='Aggregate fields from the CSDCO database by Expedition for Drupal import.')
  input_output = parser.add_argument_group(gooey_options={'columns': 1})
  input_output.add_argument('database_file', widget='FileChooser', metavar='CSDCO Database File', help='Path of the CSDCO database file.')
  input_output.add_argument('output_directory', widget='DirChooser', metavar='Save Path', help='Where to save output files.')
  args = parser.parse_args()

  if not os.path.isfile(args.database_file):
    print(f"ERROR: database file '{args.database_file}' does not exist.", flush=True)
    exit(1)
  
  if not os.path.isdir(args.output_directory):
    print(f"ERROR: output folder '{args.output_directory}' does not exist.", flush=True)
    exit(1)
  
  # Use filename if provided, else create using datetimestamp
  outfile = 'project_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
  outfile = os.path.join(args.output_directory, outfile)

  outfile_location = outfile.replace('project_data_','project_location_data_')
  outfile_location = os.path.join(args.output_directory, outfile_location)

  # List of projects to exclude from export, e.g., ocean drilling projects
  exclude_projects = ['AT15','ORCA','B0405','B0506','SBB']
    
  # List of projects to print info on for troubleshooting
  debug_projects = []

  start_time = timeit.default_timer()

  aggregate_metadata(args.database_file, outfile, exclude_projects=exclude_projects, debug_projects=debug_projects)
  export_project_location_data(args.database_file, outfile_location, exclude_projects=exclude_projects, debug_projects=debug_projects)
  
  print(f'Completed in {round(timeit.default_timer()-start_time,2)} seconds.', flush=True)


if __name__ == '__main__':
  main()
