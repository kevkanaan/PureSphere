# Géorisques

List of industrial facilities releasing pollutants.

Data from: https://georisques.gouv.fr/donnees/bases-de-donnees/installations-industrielles-rejetant-des-polluants.

## Dataset

The website provides a list of archives, one per year. Each archive contains several CSV files (some of them are not available for all years):

- `emissions.csv`: quantity of each pollutant discharged over the year by a company
- `etablissements.csv`: list of companies with their details (location, etc.)
- `Prelevements.csv`
- `Prod_dechets_dangereux.csv`
- `Prod_dechets_non_dangereux.csv` (since 2007)
- `rejets.csv` (since 2019)
- `Trait_dechets_dangereux.csv` (since 2005)
- `Trait_dechets_non_dangereux.csv` (since 2005)


You can find below the list of columns for each file.

<details>
<summary>emissions.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee_emission</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>milieu</td>
    <td>str</td>
    <td>values: Air, Eau (direct), Eau (indirect), Sol</td>
  </tr>
  <tr>
    <td>polluant</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>unite</td>
    <td>str</td>
    <td>always kg/an</td>
  </tr>
</table>
</details>

<details>
<summary>etablissements.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>numero_siret</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>adresse</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>code_postal</td>
    <td>str</td>
    <td>better to use str to keep trailing zeros</td>
  </tr>
  <tr>
    <td>commune</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>departement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>region</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>coordonnees_x</td>
    <td>float</td>
    <td>the coordinates do not seem reliable</td>
  </tr>
  <tr>
    <td>coordonnees_y</td>
    <td>float</td>
    <td>the coordinates do not seem reliable</td>
  </tr>
  <tr>
    <td>code_epsg</td>
    <td>str</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>code_ape</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>libelle_ape</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>code_eprtr</td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>libelle_eprtr</td>
    <td>str</td>
    <td></td>
  </tr>
</table>
</details>

<details>
<summary>Prelevements.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>prelevements_eaux_souterraines</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>prelevements_eaux_surface</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>prelevements_reseau_distribution</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>prelevements_mer</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
</table>
</details>

<details>
<summary>Prod_dechets_dangereux.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>code_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>libelle_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>code_departement</td>
    <td>int</td>
    <td>
        empty if it does not concern France<br>
        /!\ departments starting with a 0 are represented without the 0 (e.g. 01 is 1)
    </td>
  </tr>
  <tr>
    <td>pays</td>
    <td>str</td>
    <td>empty if it concerns France</td>
  </tr>
  <tr>
    <td>code_dechet</td>
    <td>str</td>
    <td>e.g. 07 07 08*</td>
  </tr>
  <tr>
    <td>libelle_dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>unite</td>
    <td>str</td>
    <td>tonnes/an</td>
  </tr>
</table>
</details>

<details>
<summary>Prod_dechets_non_dangereux.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>code_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>libelle_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>code_departement</td>
    <td>int</td>
    <td>
        empty if it does not concern France<br>
        /!\ departments starting with a 0 are represented without the 0 (e.g. 01 is 1)
    </td>
  </tr>
  <tr>
    <td>pays</td>
    <td>str</td>
    <td>empty if it concerns France</td>
  </tr>
  <tr>
    <td>code_dechet</td>
    <td>str</td>
    <td>e.g. 07 07 08*</td>
  </tr>
  <tr>
    <td>libelle_dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>unite</td>
    <td>str</td>
    <td>tonnes/an</td>
  </tr>
</table>
</details>

<details>
<summary>rejets.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee_rejet</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>rejet_raccorde_m3_par_an</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>rejet_isole_m3_par_an</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
</table>
</details>

<details>
<summary>Trait_dechets_dangereux.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>code_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>libelle_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>code_departement</td>
    <td>int</td>
    <td>
        empty if it does not concern France<br>
        /!\ departments starting with a 0 are represented without the 0 (e.g. 01 is 1)
    </td>
  </tr>
  <tr>
    <td>pays</td>
    <td>str</td>
    <td>empty if it concerns France</td>
  </tr>
  <tr>
    <td>code_dechet</td>
    <td>str</td>
    <td>e.g. 07 07 08*</td>
  </tr>
  <tr>
    <td>libelle_dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite_admise</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite_traitee</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>unite</td>
    <td>str</td>
    <td>tonnes/an</td>
  </tr>
</table>
</details>

<details>
<summary>Trait_dechets_non_dangereux.csv</summary>

<table>
  <tr>
    <th>column</th>
    <th>type</th>
    <th>comment</th>
  </tr>
  <tr>
    <td>identifiant</td>
    <td>int</td>
    <td>can be empty</td>
  </tr>
  <tr>
    <td>nom_etablissement</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>annee</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>code_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>libelle_operation_eliminatio_valorisation</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>code_departement</td>
    <td>int</td>
    <td>
        empty if it does not concern France<br>
        /!\ departments starting with a 0 are represented without the 0 (e.g. 01 is 1)
    </td>
  </tr>
  <tr>
    <td>pays</td>
    <td>str</td>
    <td>empty if it concerns France</td>
  </tr>
  <tr>
    <td>code_dechet</td>
    <td>str</td>
    <td>e.g. 07 07 08*</td>
  </tr>
  <tr>
    <td>libelle_dechet</td>
    <td>str</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite_admise</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>quantite_traitee</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>unite</td>
    <td>str</td>
    <td>tonnes/an</td>
  </tr>
</table>
</details>

## Wrangling

It will certainly be necessary to work on locating events, as the `coordonnees_x` and `coordonnees_y` columns in the `etablissements.csv` file are often filled with unusable values. For example, we could use an API that returns latitude and longitude from a text address.

## Scripts

Some scripts are provided in this folder to download and process the data. You can take a look at the header of a script to find out more about its purpose.

Please **run these scripts at the root of the PureSphere project** to ensure proper path management.
