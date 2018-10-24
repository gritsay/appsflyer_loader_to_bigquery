
# ETL loader for Appsflyer data. \




## About
Data loading into Bigqeury database.

## How to set up the development environment:


Firstly need to create table at Bigquery for logging details about load data

Tablename: import_appsflyer_log


id	INTEGER		
date	DATETIME	
from	DATE	
to	DATE		
count_rows	INTEGER	
table	STRING	


run command 

appsflyer_loader() {
    python ./appsflyer_loader \
    --project YOUR PROJECT NAME FOR BIGQUERY DATABASE \
    --dataset YOUR TABLENAME  \
    --app_id YOUR_APSFLYER_APP_ID \
    --api_token YOUR_APPSFLYER_API_TOKEN \
    --tables partners_by_date_report,geo_by_date_report,geo_report,partners_report,installs_report,uninstall_events_report \
    --oauth_file YOUR_GOOGLE_APPLICATION_CREDENTIALS_FILE
    
    
    
    