Steps:

1 - Install BigchainDB (Available on http://docs.bigchaindb.com/projects/server/en/latest/simple-deployment-template/index.html)

2 - Install PostgreSQL (any version)

3 - Change the following files: "config/bc_configs.json" and "config/database_configs.json"

4 - Execute the SQL script in "data-schema/relational-schema.sql" to create the the tables to database

5 - Execute the python file test.py

OBS 1: The file "test_output.txt" is one output of the execution of "test.py"
OBS 2: The dataset used is in "dataset/dataR2.csv"
OBS 3: The index file is in "files/index.json"
