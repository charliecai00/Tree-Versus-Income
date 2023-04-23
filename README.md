# Examining the Relationship Between Tree Quality and Socioeconomic Status in New York City

_This is the class project in New York University **Processing Big Data for Analytics Applications** class during 2023 Spring. All rights reserved._


## Description of the Project
This project aims to explore the relationship between the quality of trees and household income in New York City. The application plans to use two datasets - one containing information on the health, status, and trunk diameter of trees in the city and another with data on the median income levels of households across different areas of NYC. By analyzing the data, the project will determine whether there is a correlation between the quality of trees and household income across the city. We believe that the results of this application will be useful to inform policymakers and urban planners in their efforts to create more equitable and sustainable urban environments in NYC.

-----

## Prerequisites
- Upload [opencsv](./opencsv-5.7.1.jar) to NYU's Dataproc and HDFS

## Raw Data
1. Download tree data [here](https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh)
2. Download medium income [here](https://data.cccnewyork.org/data/download#0,8/66), select all Income
3. Get city-zips [here](./raw_data/city_zips.csv)
4. OR use the csv files saved in ./raw_data

## Data Cleaning
- Upload all files in ./raw_data to to NYU's Dataproc and HDFS
- lc4181 <br>
    - Run Clean with the following command <br>
    ```sh install.sh```
    - The proof of success run are saved as output*.png
- cz1906 <br>
    - Run Clean with the following command <br>
    ```sh install.sh```
    - The proof of success run are saved as Proof_*.png

## Data Profiling
- lc4181 <br>
    - Upload all ./profiling_code/lc4181/*.csv files to NYU's Dataproc and HDFS
    - Run Income with the following command <br>
    ```sh install.sh```
    - The proof of success run are saved as output*.png
- cz1906 <br>
    - Upload ./profiling_code/cz1906/tree_data_cleaned.csv files to NYU's Dataproc and HDFS
    - Run DBH with the following command <br>
    ```sh dbh_install.sh```
    - Run Health with the following command <br>
    ```sh health_install.sh```
    - The proof of success run are saved as Part1_Proof*.png

## Data Ingestion
- Upload all *.csv in ./data_ingest to NYU's Dataproc and HDFS
- Run ./data_ingest/data_ingest.py on NYU's Dataproc <br>
```spark-submit data_ingest.py```
- View results on HDFS <br>
```hd -cat output/part-00000-id.csv```
- The results are saved [here](./data_ingest/result.csv), and proof of success run are saved as output*.png

## Analysis
- Upload all *.csv in ./ana_code to NYU's Dataproc and HDFS
- Run ./ana_code/analysis.py on NYU's Dataproc <br>
```spark-submit analysis.py```
- View YARN logs for results <br>
```yarn logs -applicationId <application ID>```
- The results are saved [here](./ana_code/result.txt), and proof of success run [here](./ana_code/output1.png) and [here](./ana_code/output2.png)

## Results
- [Our result can be found at here.](./ana_code/result.txt)

## Collaborators
Contributor to this project: Charlie Cai(lc4181@nyu.edu), Stephen Zhang (stephen.zhang@nyu.edu)
