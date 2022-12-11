# This commands get final output tables from HDFS TO local machine and save it as CSV file 

hdfs dfs -get /user/cloudera/ds/COVID_FINAL_OUTPUT1/* /home/cloudera/covid_project/final_output/Top_Test.csv

hdfs dfs -get /user/cloudera/ds/COVID_FINAL_OUTPUT2/* /home/cloudera/covid_project/final_output/Top_Death.csv 

