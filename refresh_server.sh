#!/bin/bash
echo "build front script"
if [ -d "./env" ]; then
    echo "env exist"
else
    python3 -m venv ./env
    echo "build python venv into ./env"
fi
source env/bin/activate
echo "$VIRTUAL_ENV"
pip install pip --upgrade > env_lib_log.txt
pip install -r ./web_service/requirements.txt >> env_lib_log.txt
python ./refresh_server_data.py