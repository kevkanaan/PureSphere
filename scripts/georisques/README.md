# Géorisques

List of industrial facilities releasing pollutants.

Data from: https://georisques.gouv.fr/donnees/bases-de-donnees/installations-industrielles-rejetant-des-polluants.

## Dataset

The website provides a list of archives, one per year. Each archive contains several CSV files (some of them are not available for all years):

- `emissions.csv`
- `etablissements.csv`
- `Prelevements.csv`
- `Prod_dechets_dangereux.csv`
- `Prod_dechets_non_dangereux.csv` (since 2007)
- `rejets.csv` (since 2019)
- `Trait_dechets_dangereux.csv` (since 2005)
- `Trait_dechets_non_dangereux.csv` (since 2005)

## Scripts

Some scripts are provided in this folder to download and process the data. You can take a look at the header of a script to find out more about its purpose.

Please **run these scripts at the root of the PureSphere project** to ensure proper path management.
