#!/bin/bash

pip install virtualenv

mkdir venvs
cd venvs
virtualenv python3.6
cd ..

source venvs/python3.6/bin/activate

pip3.6 install google-cloud-storage
pip3.6 install google-cloud-bigquery
pip3.6 install --upgrade google-api-python-client
pip3.6 install --upgrade oauth2client
pip3.6 install --upgrade pyyaml
