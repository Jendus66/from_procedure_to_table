# from_procedure_to_table

This short Python script for getting data from SQL procedure and insert them into a table. If some exception occurs during the run, script sends a notification to smartphone via Pushover app.
The script does these actions:
1) Get credentials from config.py
2) Connection to database
3) Get courses for which questions will be gotten
4) Run SQL procedure, which returns questions summary, for every course from step 3
5) Insert the data from the SQL procedure into a table in the database 
