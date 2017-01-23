# rambleon
Ramble On

This script takes a list of persons and extracts all their movement starting from their English Wikipedia page.
The names of the persons in the list must be the same of their Wikipedia url, 

e.g. `https://en.wikipedia.org/wiki/Albert_Einstein` -> `Albert_Einstein` . An example of the list can be found in the file `list.txt`

The script returns three files as output:
* A `.tsv` file containing all the movements in a tab separated format. This file includes also the movements missing some informations (e.g. the coodrdinates)
* A `_clean.tsv` file containing only the movements having all the information in a tab separated format. 
* `.json` file: same content of the `_clean.tsv` but in `.json` format. This the file to be used with the Ramble On Navigator


Requirements:
This script requires the `KafNafParserPy` and `SPARQLWrapper` python modules.
To install them:
```
sudo easy_install pip
pip install --user KafNafParserPy 
pip install --user SPARQLWrapper
```

To run the code:
```
python ramble_on.py -l list.txt  -p pantheon_subset.txt -e -o output_movements
```

Parameters:
* `-l`  file containing the list of names
* `-o`  name of the output
* `-p` files with metadata from the Pantheon dataset (optional). This file is not necessary, but, if present, it provides the information needed by the Ramble On Navigator to filter the queries (e.g. by nation or profession)
* `-e`  extend the number of corefence chains to use

Services used:

This script relies on `Pikes`, `Nominatim` and `DBpedia`. The urls of these services can be configured in the file `config.ini`. For an heavy use of this script (e.g. dozens of biographies) we suggest to install these services locally.
