-- join movie weights and movies to get a list of top ten highest weight movies and
-- and actors for each of those movies

movies_weights = load './data/movie-weights/imdb-weights.tsv' using PigStorage('\t') 
         as (movie:chararray, year:int, weight:float);

movies_weights_sorted = ORDER movies_weights BY weight DESC;

movies_weights_limit = LIMIT movies_weights_sorted 10;

movies = load './data/movie/imdb.tsv' using PigStorage('\t') 
         as (actor:chararray, movie:chararray, year:int);

movies_weights_join = JOIN movies_weights_limit by (movie, year), movies BY (movie, year);

movies_weights_actor = FOREACH movies_weights_join GENERATE
                            movies_weights_limit::movie,
                            movies_weights_limit::year,
                            movies_weights_limit::weight,
                            movies::actor;                            
                            
RMF /tmp/hadoop/pig/movies_weights_actor;
store movies_weights_actor into '/tmp/hadoop/pig/movies_weights_actor' using PigStorage();