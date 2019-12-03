import pandas as pd
import cbsodata
import csv
from sqlalchemy import create_engine
import getpass
import os
import datetime


#Downloading entire dataset
deathdata=pd.DataFrame(cbsodata.get_data('7052eng'))
facilitiesdata=pd.DataFrame(cbsodata.get_data('7042eng'))

#Downloading metadata
deathmetadata=pd.DataFrame(cbsodata.get_meta('7052eng', 'DataProperties'))
facilitiesmetadata=pd.DataFrame(cbsodata.get_meta('7042eng', 'DataProperties'))

#getting subset################# saved at this stage for testing purpose (test mysql procedure)
deathunified=deathdata.iloc[0:69, 3:5]
#facilitiesunified=facilitiesdata.iloc[:,[1,2,261,294,316,349,382,391,155]]
################################

#link python to mysql and load data
class datamanager:

	#build connnection to mysql database
	def __init__ (self, database, username=input('insert username'), password=getpass.getpass('insert password')):
		self.engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(user=username, pw=password, db=database))
		
		try:
			#os.system('sudo systemctl start mysqld')
			self.engine.connect()
		except:
			print("OperationalError: Unable to connect to MySQL database.")
			
	#load dataframe to mysql database directly
	def insert_sql_data(self, data, tablename):
		try:
			data.to_sql(tablename, con=self.engine, if_exists="append")
			return 0
		except:
			print("LoadingError: Unable to load dataframe to MySQL database.")
			return 1
		finally:
			self.engine.connect().close()

	#load dataframe to csv file and then import to mysql
	def download_csv_data(self, data, tablename):
		self.filename='~/data_analytics_project/Scripts/rawdata/'+ tablename + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
		try:
			data.to_csv(self.filename +'.csv')

			try:
				df=pd.read_csv(self.filename +'.csv', sep=',')
				df.to_sql(tablename, con=self.engine, if_exists="replace")
				return 0

			except:
				print("ImportingError: Unable to import csv file to mysql.")
				return 1

			finally:
				self.engine.connect().close()
		except:
			print("LoadingError: Unable to download csv file.")
			return 1

		


#load file to mysql database staging and unified 


loadstaging=datamanager(database="STAGING")

####################################
#loadunified=datamanager(database="UNIFIED")
####################################

#loadstaging.insert_sql_data(deathdata,"stagingdeath")
#loadstaging.insert_sql_data(facilitiesdata,"stagingfacilities")

loadstaging.download_csv_data(deathdata,"stagingdeath")
loadstaging.download_csv_data(facilitiesdata,"stagingfacilities")


########################################
#loadunified.insert_sql_data(deathunified,"Death")
#loadunified.insert_sql_data(facilitiesunified,"Facilities")
########################################






 
