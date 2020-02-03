import pandas as pd
import cbsodata
import csv
from sqlalchemy import create_engine
import getpass
import os
import datetime
import sys


#Downloading entire dataset
deathdata=pd.DataFrame(cbsodata.get_data('7052eng'))
facilitiesdata=pd.DataFrame(cbsodata.get_data('7042eng'))

#Downloading metadata
deathmetadata=pd.DataFrame(cbsodata.get_meta('7052eng', 'DataProperties'))
facilitiesmetadata=pd.DataFrame(cbsodata.get_meta('7042eng', 'DataProperties'))

#link python to mysql and load data
class datamanager:


	#build connnection to mysql database
	#def __init__ (self, database, username=input('insert username'), password=getpass.getpass('insert password')):
	def __init__ (self, database):
		f=os.path.join(os.path.expanduser('~'), 'data_analytics_project', 'Scripts', 'authentication.txt')
		F=open(f)
		lines=F.readlines()
		self.username=lines[0].strip()
		self.password=lines[1].strip()
		self.engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(user=self.username, pw=self.password, db=database))
		F.close
		
		try:
			#os.system('sudo systemctl start mysqld')
			self.engine.connect()
		except:
			print("OperationalError: Unable to connect to MySQL database.")
			
	#load dataframe to mysql database directly
	def insert_sql_data(self, data, tablename):
		try:
			data.to_sql(tablename, con=self.engine, if_exists="append")			
		except:
			print("LoadingError: Unable to load dataframe to MySQL database.")
			
		finally:
			self.engine.connect().close()

	#load dataframe to csv file and then import to mysql
	def download_csv_data(self, data, tablename):
		self.filename='~/data_analytics_project/Scripts/rawdata/'+ tablename + datetime.datetime.now().strftime("%Y%m%d%H%M")
		try:
			data.to_csv(self.filename +'.csv')
		except:
			print("LoadingError: Unable to download csv file.")


		try:
			df=pd.read_csv(self.filename +'.csv', sep=',')
			df.to_sql(tablename, con=self.engine, if_exists="append")	
		except:
			print("ImportingError: Unable to import csv file to mysql.")

		finally:
			self.engine.connect().close()


#load file to mysql database staging and unified 

loadstaging=datamanager(database="STAGING")

#loadstaging.insert_sql_data(deathdata,"stagingdeath")
#loadstaging.insert_sql_data(facilitiesdata,"stagingfacilities")

loadstaging.download_csv_data(deathdata,"stagingdeath")
loadstaging.download_csv_data(facilitiesdata,"stagingfacilities")
