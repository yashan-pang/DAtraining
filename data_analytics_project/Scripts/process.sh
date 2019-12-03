#This is used for control the entire process

#user give command to start the loading process
echo "Load new data? (yes/no)"
read loadControl

#new laod and ETL begin and renew every 10 mins
while [ "$loadControl" == "yes" ]; do

#python code to load data and confirm complete 
	python3 ./data_analytics_project/Scripts/data_manager.py

#start etl and confirm complete
	if [ "$(sh ./data_analytics_project/Applications/etl.sh)" == "1" ]; then 

#send email to client to notify the new load
			#echo "New load is completed." | mail -v -s "New Load" yashan.pang@devoteam.com
		echo "New load is completed."

#start spotfire
		sh ./data_analytics_project/Applications/spotfire.sh
	else

#if something wrong with etl, send mail to notify 
			#cho "ETL error." | mail -v -s "Error" yashan.pang@devoteam.com
		echo "ETL error."
		break
		
	fi

#run the loop every 10 mins
	sleep 600
done

echo "Exiting."


