# Steps to run the application

## Data ingest
1. Download tree data [here](https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/pi5s-9p35)
2. Download medium income [here](https://data.cccnewyork.org/data/download#0,8/66)
3. Get city-zips [here](./data_ingest/city_zips.csv)

## Cleaning code


## Profiling code

## Analytics
- Run ./ana_code/analysis.py on NYU's Dataproc <br>
```spark-submit analysis.py```
- View YARN logs for results <br>
```yarn logs -applicationId <application ID>```
- The results are saved [here](./ana_code/result.txt), and proof of success run [here](./ana_code/output1.png) and [here](./ana_code/output2.png)

## [Link to Github]()