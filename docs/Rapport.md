### PureSphere - Data engineering project report
Team:
- Tom DELAPORTE
- Kevin KANAAN
- Jorick PEPIN

### Objectives
The goal of PureSphere is to collect data to check how French industrial sites impact the quality of the air and the water in their surrounding areas.

Our objective is to answer the 2 following questions for France in 2021:
- What are the zones for which we have information about the air quality and the water quality?
- Can we see the impact of industrial sites on their surrounding area in terms of air and water quality?

### Data ingestion 
Our [ingestion pipeline](../dags/ingest.py) downloads data from 3 different sources:
- [Géorisques](https://georisques.gouv.fr/donnees/bases-de-donnees/installations-industrielles-rejetant-des-polluants): data about French industrial facilities releasing pollutants
- [Hub'eau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau#/): analysis of the water quality of French rivers, lakes, watercourses.
- [Geod'air](https://www.geodair.fr/): analysis of air quality in France.

The pipeline `ingest.py` is responsible for downloading the data from the internet and storing it in the [landing zone](../data/landing/). We had a lot of troubles finding the proper way to download the data. For air and water quality measurements, data is very large so we quickly reached the limit of availables API. For the water quality, several requests are sent to collect the data by playing with Hub'eau API parameters. At the moment we built the ingestion pipeline, only 2021 data were available. That's why our analysis is focusing on 2021 even if Geod'air and Géorisques pipelines also fetch data for 2022 (even 2023 for Geod'air). For air quality data, they are available in real time on [data.gouv.fr](https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/) without limitations on the number of calls. Géorisques data are also available at data.gouv.fr.

Air quality data consists of hourly averaged measures for thousands of air quality sensors in France. The yearly volume of air quality data in raw CSV files is about 3.5 GB. For the sake of reducing disk usage, we decided to use Parquet to encode heavy files, reducing this size to roughly 80 MB, which is very nice! The same principle is applied to water quality data. There is no need for this encoding with Géorisques data so we kept them as CSV files.

To have more details about the specifities of each dataset, you can read the ReadMe inside [`air_quality`](../dags/air_quality/README.md), [`georiques`](../dags/georisques/README.md) and [`water_api`](../dags/water_api/) dags subfolders.

We haven't developped a proper mechanism within the ingestion pipeline to deal with offline working. If you don't have an Internet connection this pipeline will fail. Nevertheless, if you already have some data in the landing zone, you can directly launch the [wrangling pipeline](../dags/wrangling.py).

### Staging
The [wrangling pipeline](../dags/wrangling.py) is responsible for staging data stored in the [landing zone](../data/landing/). For the cleaning steps, the different data sources required tailored treatments. The cleaning is made using Pandas.

Georisques data provided us with many files but only 3 were relevants for the sake of PureSphere: `emissions.csv`, `etablissements.csv` and `rejets.csv`. Then useless columns are dropped from these files, its column names and content is stripped to get rid of trailing whitespaces. The most difficult part for Georisque data is that the industrial facilities are located using their adresses and not latitude and longitude as in Geod'air and Hub'eau data. We used Geopy to convert the adresses to coordinates. As this steps takes a lot of time (more than 3 hours), we cached the adresses once they are computed in [a JSON file](../data/staging/georisques/addresses.json). We could've used MongoDB to cache this JSON file but we didn't as we ran out of time. If the address is present in the cached file, then it is directly retrieve from it. Otherwise, the address it retrieved using Geopy and stored in the cached file.

Geod'air landing data consists of daily files containing hourly averaged measurements for all the air quality monitoring sensors. During the first steps of the cleaning, we kept this architecture and loop through all the daily files to clean independently all of them. The file containing the [station metadata](../data/landing/) doesn't required much cleaning. We keep all its columns even if they aren't all relevant to answer our questions but these metadatas might be useful for further developments of the project. Then, invalid measurements are removed (measurements made by unreliable station, measurements tagged as falsy, etc.). To compute a daily average for each type of pollutant and measurement station, each daily file is aggregated by station and pollutant type and the daily mean, min, max, std and number of measures are computed. The most important step for Geod'air is to merge all the daily files. First, we did it looping through all the daily aggregated files and appending them to a dataframe. This step was too long. To reduce the time, we decided to use Apache Spark. Using a master linked to a single slave that can use 3 cores and 4 GB of RAM, we were able to divide by 10 the time required to merge the daily files. Hub'eau data received quite the same cleaning steps that air quality data so we won't detail it. Cleaning the data was the longest and toughest part of the project.

At each step, we stored the data in transient folders so that we keep track of what we did before and we don't have to re-run all the steps in case of a failure when building the pipeline. At the end of the wrangling and enrichment, we store the cleaned data in a persistent Postgres database called staging. Some constraints (primary key, foreign key) are applied between the different tables coming from the same source so that we are sure to have no mismatch when moving to production. Some entries were dropped at this moment (for example a data refering to a station not listed in the station metadata file).

### Production
To answer the first question, we simply retrieves all the water quality stations and air quality stations that were working in 2021 and merge them in a single table.

 Then, to answer the second question, we have created a graph database using Neo4J. The hardest and trickiest part is to map the industrial sites to their surrounding monitoring stations based on their coordinates. To do this, we have used Geopy and its distance functions. Considering the large number of stations, we decided arbitrarily that a station is monitoring an industrial site if it is located in a 10km radius zone around the site. We stored the result of this mapping inside the table called `industrial_sites_monitoring_stations_2021`. All the air quality stations, regardless of their functionning year, are already mapped to an industrial site so that it is already done for later analysis. For the sake of accelerating the pipeline, this table is exported as a CSV file in the [production data](../data/production/industrial_sites_monitoring_stations_2021.csv). Inside our Neo4J database, we have 3 entities: IndustrialSites, Station:WaterQuality and Station:AirQuality. Each of them carries enough data to retrieve the measurements from the measurements tables stored inside the staging database and answer our question.

 ### Future works
 The first step to properly answer our questions would be to develop data visualisations. Then, further works need to be done to be able to run an analysis for a given period of time. Finally, we should use MongoDB to cache our data during the staging.

 ### Difficulties encountered
 This project took a lot of time to be completed because debugging is hard when you're pipeline fails after 3 hours of computing. Tom had to add another 8 BG of RAM inside his computer otherwise he wasn't able to run the pipeline at all.

 ### Acknowledgement
 We kindly thank Riccardo Tommasini for his help throughout the project! 