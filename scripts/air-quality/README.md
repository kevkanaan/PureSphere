# Geod'air Data

[Geod'air](https://www.geodair.fr/) is the national air quality database. Managed and implemented by [Ineris](https://www.ineris.fr/fr) as part of the [Central Air Quality Monitoring Laboratory](https://www.lcsqa.org/fr), it provides reference data and statistics on air quality in France. The source data comes from the monitoring system operated in each region by the approved air quality monitoring associations.


## Dataset
Even if Geod'air provides an API, it's restriction rules are too restrictive for us to use it to fetch the data. However, we can access API's data from [data.gouv.fr](https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/).

## Script
For the moment, 2 methods are provided:
- ```download_stations_details()``` retrieves metadata about measuring stations.
- ```download_daily_reports(years: list[int])``` retrieves daily **correct** measures for all measuring stations available that day. Once the function is executed, there should be a folder named *{YYYY}* [here](../../data/air-quality/). Inside this folder are located all the files containing daily raw measures. To minimize the weight of these raw measurements files, they are stored in Apache Parquet format. For the sake of efficiency, you can use Apache Arrow to work on these files. 

## Dependencies
Make sure to install the following libraries:
- BeautifulSoup
- Pandas
- io
- os
- requests
- pyarrow
- xlrd

---
Please **run these scripts at the root of the PureSphere project** to ensure proper path management.