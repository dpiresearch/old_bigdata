-- 
-- This script will sum the scores over the a 30 period and group them by country, category, location,
-- and keyword
--
-- Input variables
--   inputdir - directory pointing to the input data
--   refdir - reference data (location and category human readable names
--   outputdir - directory to write output to
--

--
-- TODO:  Need to specify range of dates to get 30 days instead of using * 
--


SET default_parallel 50;

D1 = LOAD '$inputdir/*/scores/*' USING PigStorage() as (day:int, country:chararray, catid:int, locid:int,kw:chararray,kwcount:long,score:float);

D11 = FOREACH D1 GENERATE *;
-- DUMP D11;

--
-- Need country also
--
D11G = GROUP D11 BY (country, catid, locid, kw);
D11F = FOREACH D11G GENERATE group.country as country, group.catid as catid, group.locid as locid, group.kw as kw, SUM(D11.score) as score, 0 as trending, (double) 0.0 as trending_score;

DESCRIBE D11F;
-- DUMP D11F;

--
-- Start Trending searches
--
T1 = LOAD '$inputdir/$daydate/scores/*' USING PigStorage() as (day:int, country:chararray, catid:int, locid:int,kw:chararray,kwcount:long,score:float);

T11 = FOREACH T1 GENERATE *;
T11G = GROUP T11 BY (country, catid, locid, kw);

T11F = FOREACH T11G GENERATE group.country as country, group.catid as catid, group.locid as locid, group.kw as kw, 1 as trending, SUM(T11.score) as trending_score;

DESCRIBE T11F;
-- DUMP T11F;
-- STORE T11F INTO '$inputdir/$daydate/trending_sum';
-- STORE D11F INTO '$inputdir/$daydate/sum';

DTJ = JOIN D11F BY (country, catid, locid, kw) LEFT, T11F BY (country, catid, locid, kw);
-- DTJF = FOREACH DTJ GENERATE D11F::country as country, D11F::catid as catid, D11F::locid as locid, D11F::kw as kw, D11F::score as score, (T11F::trending==1?T11F::trending:0) as trending, (T11F::trending_score > 0.0?T11F::trending_score:D11F::trending_score) as trending_score;
-- DTJF = FOREACH DTJ GENERATE D11F::country as country, D11F::catid as catid, D11F::locid as locid, D11F::kw as kw, D11F::score as score, (T11F::trending==1?T11F::trending:D11F::trending) as trending;
DTJF = FOREACH DTJ GENERATE D11F::country as country, D11F::catid as catid, D11F::locid as locid, D11F::kw as kw, D11F::score as score, T11F::trending as trending_1, D11F::trending as trending_2, T11F::trending_score as ts_1, D11F::trending_score as ts_2;

-- DTJF = FOREACH DTJF GENERATE country, catid, locid, kw, score, (trending_1 is null?trending_2:trending_1), (ts_1 is null?ts_2:ts_1);
DTJF = FOREACH DTJF GENERATE country, catid, locid, kw, score, (trending_1 is null?'false':'true'), (ts_1 is null?ts_2:ts_1);

-- DTJFF = FOREACH DTJF GENERATE country, catid, locid, kw, score,(trending != ''?trending:'false'),(trending_score != ''?trending_score:'0.0');

-- STORE DTJFF INTO '$outputdir/$daydate/sum';
STORE DTJF INTO '$outputdir/$daydate/sum';

