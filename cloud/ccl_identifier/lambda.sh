#!/bin/bash

export AWS_PROFILE=dmrc
export AWS_REGION=ap-southeast-2
rm -r upload
rm upload.zip
rm requirements.txt

source ./env/bin/activate

mkdir upload

rsync -av . ./upload --exclude="env" --exclude="upload" --exclude="env-local"

pip freeze > requirements.txt --no-cache-dir
pip install -r requirements.txt -t ./upload --no-cache-dir

cd upload

zip -r upload.zip *

aws lambda update-function-code --function-name arn:aws:lambda:ap-southeast-2:519969025508:function:moat_ccl_identifier --zip-file fileb://upload.zip

cd ..

rm -r upload
#rm requirements.txt