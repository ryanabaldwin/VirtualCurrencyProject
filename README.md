# VirtualCurrencyProject
Personal project that replicates a process created in a job of mine. This project is a relatively simple Python script that inserts CSV files containing transaction data into a SQL database.

I initially built a mapping in Informatica Cloud that takes CSV files with transaction data (one for transactions, one for user credit), makes the necessary transformations, and upserts them into a SQL database. I wanted to rebuild this project in Python as a way to improve my coding skills on my own time. This is only part 1; part 2, coming soon, is a Python script that calculates which departments need to be charged for the amounts of virtual currency that were spent at events.

To use: download MySQL on your computer, then run the scripts in the VN_Tables.sql file. Next, you will need to alter the variables at the top of the Python file. Change the SFTP and MySQL connection variables to match the hostnames, usernames, and passwords/file paths that match your own computer and MySQL user. Before running the script, make sure that the sample CSV files in this repository are downloaded and saved to the right directories. Enjoy!
