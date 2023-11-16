# Geod'air Data

[Geod'air](https://www.geodair.fr/) is the national air quality database. Managed and implemented by [Ineris](https://www.ineris.fr/fr) as part of the [Central Air Quality Monitoring Laboratory](https://www.lcsqa.org/fr), it provides reference data and statistics on air quality in France. The source data comes from the monitoring system operated in each region by the approved air quality monitoring associations.

## Dependencies
Make sure the script are executing properly, make sure to install the following libraries:
- BeautifulSoup
- Pandas
- io
- os
- requests
- pyarrow
- xlrd

---
Please **run any script at the root of the PureSphere project** to ensure proper path management.

## Dataset
Even if Geod'air provides an API, it's restriction rules are too restrictive for us to use it to fetch the data. However, we can access API's data from [data.gouv.fr](https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/).

## download.py script
2 methods are provided:
- ```download_stations_details()``` retrieves metadata about measuring stations.
- ```download_daily_reports(years: list[int])``` retrieves daily **correct** measures for all measuring stations available that day. Once the function is executed, there should be a folder named *{YYYY}* [here](../../data/air-quality/). Inside this folder are located all the files containing daily raw measures. To minimize the weight of these raw measurements files, they are stored in Apache Parquet format. For the sake of efficiency, you can use Apache Arrow to work on these files.

## Landing zone
**Let's describe the content of the file contained within the landing zone.**

### [stations_metadata.csv](data/landing/air-quality)
This file contains tons of relevant information regarding air quality monitoring stations.

#### Columns
| Attribute | Format / Possible value(s) | Description|
|:----------|:-------|:-----------|
|GMLID |STA-FRxxxxx | Geographic Markup Language identifier |
|LocalId | STA-FRxxxxx | Identical to GMLID |
|Namespace | FR.LCQSA-INERIS.AQ | Namespace |
|Version | {1,2,3,4,5} | Database version |
|NatlStationCode | FRxxxxx | National station code |
|Name | String | Name of the station
|Municipality | String | Municipality where the station is located | 
|EUStationCode | FRxxxxx | European station code |
|ActivityBegin | DD/MM/YYYY hh:mm | Date at which the air quality monitoring activity started |
|ActivityEnd | DD/MM/YYYY hh:mm | Date at which the air quality monitoring activity ended (Null if the station is still monitoring air quality) | 
|Latitude | xx.xxx | Latitude of station's location |
|Longitude | xx.xxx | Longitude of station's location |
|SRSName | urn:ogc:def:crs:EPSG::4326 | [Spatial Reference System](https://en.wikipedia.org/wiki/Spatial_reference_system) |
|Altitude | Integer | Altitude of the station |
|AltitudeUnit | http://dd.eionet.europa.eu/vocabulary/uom/length/m | [Meters](http://dd.eionet.europa.eu/vocabulary/uom/length/m) | 
|AreaClassification | http://dd.eionet.europa.eu/vocabulary/aq/areaclassification/{rural|rural-nearcity|rural-regional|rural-remote|suburban|urban} | Type of area where the station is located |
|BelongsTo | FR.LCSQA-INERIS.AQ/NET-FRxxxA | |

### Measurement reports
Measurement reports are stored in Apache Parquet format within the landing zone. Their names follow the format FR_E2_YYYY_MM_DD.

The files describe the hourly average concentrations of [regulated pollutants](https://www.geodair.fr/sites/default/files/media-files/2022-05/liste_polluants.pdf) monitored by automatic measuring devices installed at fixed stations. These data are made available as and when they are acquired, using a process similar to that used for reporting. as they are acquired, using a process similar to that used for European regulatory reporting of air quality data. To have more information regarding the content of this file, please refer to the [European Comission website](https://environment.ec.europa.eu/topics/air/air-quality/data-and-reporting_en) or this [note](https://www.data.gouv.fr/fr/datasets/r/e72510b3-ed3b-450f-987b-7d6a250636b0).
