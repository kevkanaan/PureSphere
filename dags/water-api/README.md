# Water API

List of physico chemical analysis of water quality

Data from: https://hubeau.eaufrance.fr/page/api-qualite-cours-deau#/

## Dataset

The website provides a list of results during an API call.The API call can be made on a specific location or on a specific start and end date. By default the maximum number of results for the analysis is 1000. We can force our way to 10000 but we can't go more than this. We will be making specific call with spatio-temporal restrctions to use this data and compare it to the other datasets. The API will return us data that are going to be written in three different csv files. For each endpoint a csv file :

- `analysispc.csv`
- `operationpc.csv`
- `stationpc.csv`

## Functions

Get_analysispc
Get_operationpc
Get_stationpc

Get_analysispc_location
Get_operationpc_location
Get_stationpc_location

Get_analysispc_location_date

## Dataset Limitations

The maximum number of rows that we can get for a request is 20 000. To get the rest of the results we need to try and play on some conditions. 
So what we did is that we selected the four most important chemical components and we filtered in our API Call to get them. 
The chemical components' codes are : 1319,1350,1383,1386

## Scripts

Some scripts are provided in this folder to download and process the data. You can take a look at the header of a script to find out more about its purpose. You can also find documentation for each function

Please **run these scripts at the root of the PureSphere project** to ensure proper path management.
