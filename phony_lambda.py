import time
import database

while True:
    database.connect_to_rds() 
    time.sleep(15)
