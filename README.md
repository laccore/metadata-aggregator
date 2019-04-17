# CSDCO Metadata Aggregator

This script takes information from the CSDCO SQLite database and aggregates data from various tables into outputs for loading into the [CSDCO Drupal website](https://csdco.umn.edu/projects).

It has one required input, the path to the sqlite database, and files are exported to the same directory the script is run from.

## Example usage
```python
python csdco-metadata-aggregator.py /path/to/csdco_database.sqlite3
```

## Aggregation
### Project Data
All data is aggregated based on the Expedition code (**PROJECT ID** in the output file). Location, named features, and PIs are pulled from various columns in the ```boreholes``` table, where there are many rows (boreholes) for a given project.
* **LOCATION**: Country and state/provinces are deduplicated, sorted (based on name), and combined into one field.
* **NAMED FEATURE**: Feature names are deduplicated and sorted.
* **INVESTIGATOR**: PIs are deduplicated, but are kept in a first-seen first-position order, because order and hierarchy matter for academic publications.

This data is then joined (key = Expedition) with data from the ```projects``` table, the columns are ordered as our Drupal table expects, and the csv file is written to disk.

### Location Data
Location data (latitude, longitude, elevation), alongside lots of other project metadata, is used to generate maps and fill in informational tables that live alongside project information on Drupal.

The data is pulled from the ```projects``` table and exported with renamed column titles (that Drupal expects).
