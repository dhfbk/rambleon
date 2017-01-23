# rambleon
Ramble On

Requirements:
```
pip install --user KafNafParserPy 
pip install --user SPARQLWrapper
```

To run the code
```
python ramble_on.py -l list.txt  -p pantheon_subset.txt -e -o output_movements_all
```

Parameters:
* `-l`  file containing the list of names
* `-o`  name of the output
* `-p` files with metadata from the Pantheon dataset (optional)
* `-e`  extend the number of corefence chains to use
