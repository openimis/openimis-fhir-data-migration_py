# openIMIS FHIR data migration tool 

## Content
* [Description](#description)
* [Configuration Options](#configuration-options)
* [Example of usage](#example-of-usage)
* [Author](#author)

## Description
This repository holds the files for the transfer tool from openIMIS to a FHIR database in PostgreSQL as well as a json file creator. 

The module takes the data from [api_fhir_r4](http://localhost:8000/api_fhir_r4/), 
and allows to create json files and an FHIR database of this data.

To be able to use it, the openIMIS [backend server](https://github.com/openimis/openimis-be_py) has to be installed.

The link to the openIMIS wiki page of the data migration tool can be found [here](https://openimis.atlassian.net/wiki/spaces/OP/pages/1554448385/openIMIS+FHIR+data+migration+tool).

## Configuration Options

To fully interact with the console in PyCharm the user has to enable the “Emulate terminal in output console” checkbox. 
This can be found under: Run > Edit Configurations… > Execution (drop-down-menu) > Emulate terminal in output console.
                                                                                                                                                                                                                                                                                                                               
## How to use it

### Creation of JSON Files
* Start the openIMIS backend server
* Run the [data_migration.py](https://github.com/openimis/openimis-fhir-data-migration_py/blob/master/data_migration.py) script 

On the terminal:
``` 
python data_migration.py  
```
On PyCharm:

Simply click Run

* Follow the instructions on the console to create the JSON files
* A folder named "json_files" is created which contains all JSON files

### Create Database Tables
* Download [PostgreSQL](https://www.postgresql.org/)
* Create an user or use the default user called postgres
> The easiest way is to use pgAdmin, a web application PostgreSQl offers  
* Create a new database or use an existing one
* Start the openIMIS backend server
* Run the [data_migration.py](https://github.com/openimis/openimis-fhir-data-migration_py/blob/master/data_migration.py) script as above
* Follow the instructions on the console to create the database tables

>To connect to a local database, just press ENTER when the host and port is asked. 

It is possible to create all tables at once or to decide which table should be created separately.

### Update the files and the tables
To create updated files and tables it is not necessary to delete the files and the folder or to drop the tables by hand.
The program automatically deletes/drops the created files/tables and creates new ones with the latest data from the API.

## Author
Faris Ahmetasevic - faris.ahmetasevic@hotmail.com
