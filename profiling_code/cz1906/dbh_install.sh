rm *.class DBH.jar
hdfs dfs -rm -r ./output/

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . DBHMapper.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . DBHReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . DBH.java

jar -cvf DBH.jar *.class
hadoop jar DBH.jar DBH tree_data_cleaned.csv ./output/

hdfs dfs -cat ./output/part-r-00000
