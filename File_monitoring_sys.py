import multiprocessing  as mp
from multiprocessing import Pool
import os
import time
import mysql.connector

"""Function to check the count of the file"""
def file_wc(fname):
        with open('/home/vaibhav/Desktop/Input_python/'+ fname) as f:
            count = sum(1 for line in f)            
        return (fname,count)    
     
class file_audit:
    
    def __init__(self):      
        """Initialising the constructor for getting the names of files 
        and refrencing the outside class function"""
        
        folder = '/home/vaibhav/Desktop/Input_python'
        self.fnames = (name for name in os.listdir(folder))
        self.file_wc=file_wc   
   
    def count_check(self):
        "Creating 4 worker threads to check the count of the file parallelly"
        
        pool = Pool(4)
        self.m=list(pool.map(self.file_wc, list(self.fnames),4))
        pool.close()
        pool.join()

    def database_updation(self):
        """To maintain an entry in the database with details 
        like filename and recrods present in the file"""
        
        self.db = mysql.connector.connect(host="localhost",user="root",password="root",database="python_showtime" )
# prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        query_string = ("INSERT INTO python_showtime.audit_capture"
                "(name,records)"
                "VALUES(%s,%s)")
        #data_user = (name,records)
        for each in self.m:    
            
            self.cursor.execute(query_string, each)
            self.db.commit()
        self.cursor.close()
        start_time = time.time()
        print("My program took", time.time() - start_time, "to run")
 
#if __name__ == '__main__':
x=file_audit()
x.count_check()  #To check the count by sprawning multiple processes
x.database_updation() #To maintain the entry in the database
