movies = load './data/movie/imdb.tsv' using PigStorage() 
         as (actor:chararray, movie:chararray, year:int);
         
movies_group = GROUP movies by actor;
movies_count = FOREACH movies_group generate group AS actor, COUNT(movies) AS count;
movies_sorted = ORDER movies_count BY count DESC;
movies_limit = LIMIT movies_sorted 10;

--dump movies_limit;

RMF /tmp/hadoop/pig/movie/ative-actors;
STORE movies_limit INTO '/tmp/hadoop/pig/movie/ative-actors' USING PigStorage();
          