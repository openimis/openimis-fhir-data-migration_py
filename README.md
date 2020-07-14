# openimis-fhir-data-migration_py

##Content
* [Description](#description)
* [Configuration Options](#configuration-options)
* [Example of usage](#example-of-usage)
* [Author](#author)


## Description
This repository holds the files for the transfer tool from openIMIS to a FHIR database in PostgreSQL as well as a json file creator. 

The module takes the data from [api_fhir_R4](http://localhost:8000/api_fhir_R4/), 
and allows to create json files and an FHIR database of this data.

To be able to use it, download the openIMIS applications.

Links:

[openIMIS initiative YouTube channel](https://www.youtube.com/channel/UCujhZgz_6EFihAvYT_tD34Q)

[openIMIS wiki page](https://openimis.atlassian.net/wiki/spaces/OP/pages/786104344/Installation+and+Country+Localisation) 

## Configuration Options
###Terminal
Simply start the database_tool.py script.

###PyCharm
1. Navigate to the Run > Edit Configurations ... window
2. Check the "Emulate terminal in output console" field                                                                                                                                                                                                                                                                                                                               |

## Example of usage

###Create JSON Files
1. Start the openIMIS backend server
2. Run the database_tool.py script
3. Follow the instructions on the console

###Create Database Tables
1. Download [PostgreSQL](https://www.postgresql.org/)
2. Create or use the default User 
3. Create a new Database or use an existing one
4. Start the openIMIS backend server
5. Run the database_tool.py script
6. Follow the instructions on the console

## Author
Faris Ahmetasevic - faris.ahmetasevic@hotmail.com
