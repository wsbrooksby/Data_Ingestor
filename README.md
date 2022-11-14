# Data_Ingestor

This is a script that is meant to take the contents of itunes EPF files and upload them to a MySQL database.
It does this by gathering the metadata from the header of each file regarding column names, data types, and primary keys,
then building a Pandas dataframe to parse and upload the data.

## Installation

This script requires that the 'config.yaml' script is completed before it can run and upload data.
	- To create this file, simply run 'data_ingestor.py' once (in python 3). It will build a default 'config.yaml' file and save it in the running directory.
	- Afterwards, open the file and fill in the missing info in the 'db_info' section for connecting to your database.

Run `pip install -r requirements.txt` to make sure that you have compatible versions of the dependencies.


## Usage
Add EPF files that you want uploaded to your database to the `./inboxes/ready/` folder. Then open a command prompt and run 'data_ingestor.py' in python 3 from the directory where it is located. After it finishes parsing a file, the file is moved to `./inboxes/finished/` with "_datetimestamp" appended to the filename to keep it unique.
	- The name of the file is used as the table name in MySQL, so make sure not to include any file extensions. If refeeding a failed or finished record, make sure to remove the timestamp from the file name.
	- Any files that failed to be uploaded are moved to `./inboxes/finished/` with "_datetimestamp" appended to the filename. You can check the logs for details on why they failed.
	
----------------------------------
Author: William Brooksby
wsbrooksby@gmail.com
