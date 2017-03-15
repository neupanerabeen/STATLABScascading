#!/bin/bash
echo start
echo -----------

# Ensuring that the files !exists in local for input from hadoop
rm -r /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/*
rm -r /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/dataForHive
rm -r /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/outputWithHeader/*

# Loading data from hive to local
hive -e "insert overwrite local directory '/home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive' select emp_class, follow_up_medium, follow_up_intense, iir, adj_income, roi, loan_amt from appscore2;"

# Converting to tsv format
cat /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/* > /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/input
sed 's#\x01#\t#g' /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/input > /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/input1

# adding headers with data
cat /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/ModelHeader.tsv /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/input1 > /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/input2

# Ensuring that the file exists in hadoop for input from local
hadoop fs -rmr /user/hdmasteruser/data/Input.tsv

# Move the tsv to hadoop
hadoop fs -put /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/input2 data/Input.tsv

# Ensuring that the file exists in hadoop for output
hadoop fs -rmr out/classify

# Start the scoring engine
hadoop jar /home/hdmasteruser/Downloads/pattern-wip-1.0/pattern-examples/build/libs/pattern-examples-1.0.0-wip-dev.jar data/Input.tsv out/classify --pmml /home/hdmasteruser/techieventuresFiles/statlabs_files/pmml_with_numbers.xml

#get the output to local file system
hadoop fs -get /user/hdmasteruser/out/classify/part-* /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/outputWithHeader/

#merge all part* files to part
cat /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/outputWithHeader/* > /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/outputWithHeader/part

# Get last column of the last row of the output
awk 'NR>1' /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/outputWithHeader/part > /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/dataForHive

# Store the data to hive appscore_result
hive -e "load data local inpath '/home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/dataForHive' overwrite into table appscore_result;insert into table appscore select appscore1 .* , appscore2.*, appscore_result.risk from appscore1 , appscore2, appscore_result;"

# moving all data from operational tables to training data
# hive -e "insert into table appscore select appscore1 .* , appscore2.*, appscore_result.risk from appscore1 , appscore2, appscore_result;"

#clearing cache
rm -r /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/inputHive/*
rm -r /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/dataForHive
rm -r /home/hdmasteruser/Desktop/StatLabs_Scoring_Engine/cache/outputWithHeader/*
echo -----------
echo completed
