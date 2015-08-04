# csv2elastic-py

NOT PRODUCTION READY

simple python script to load csv data into an elasticsearch index with some data manipulating &amp; formatting options

requirements: official elastic python client https://elasticsearch-py.readthedocs.org/en/master/

tested on python3 with elasticsearch 1.5.x

## usage
    python csv2elastic.py data.csv [config.json]

configuration example in config folder

if no config file specified, the script will ask for required options
