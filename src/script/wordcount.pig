text = load '<local-dir>/hadoop-class-example/data/state_of_union/obama1.txt' using TextLoader();

tokens = foreach text generate FLATTEN(TOKENIZE($0)) as word;

wordgroup = group tokens by word;

wordcount = foreach wordgroup generate group as word, COUNT($1) as count;

store wordcount into '/Users/hluu/hadoop/wordcount' using PigStorage();
