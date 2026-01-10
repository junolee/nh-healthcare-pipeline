# NH Healthcare Pipeline


Dashboard: https://nh-healthcare.streamlit.app


## INGEST LAMBDA

#### Steps to run ingest locally:

Optional: 
- Create & activate virtual env (in ingest directory to create venv file there)
  - start env name with `venv` for .gitignore to ignore the created `.venv*`
- Configure .env in ingest directory with relevant env variables
- Install requirements.txt
- Run local_run.py

```
cd ./src/ingest
python -m venv venv_gdrive_to_s3
source venv_gdrive_to_s3/bin/activate
```

```
pip install -r requirements-local.txt
```
```
python local_run.py
```
To deactivate environment:
```
deactivate
```

#### Deploying to lamdba
```
# 1. Create a new directory for the deployment package
mkdir lambda_package
cd lambda_package

# 2. Install dependencies directly into this directory
pip install -t . -r ../requirements.txt -q

# 3. Copy your specific code files into this directory
cp ../gdrive_to_s3.py .
cp ../lambda_function.py .

# 4. Remove unnecessary files to keep the zip small (optional but recommended)

find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

rm -rf bin

# 5. Zip the contents of the directory (not the directory itself)
zip -r ../function.zip .
zip -r -q ../function.zip .


cd ..

# 6. Deploy (Optional - if you have AWS CLI installed)

aws lambda update-function-code --function-name gdrive_to_s3 --zip-file fileb://function.zip

# 8. Cleanup
rm -rf lambda_package function.zip
```

## ETL

#### Steps to run ETL jobs locally:

```
cd ./src/etl
python -m venv venv_nh_etl
source venv_nh_etl/bin/activate
```
```
pip install -r requirements-local.txt
```
```
python local_runner.py
```
To deactivate environment:
```
deactivate
```
