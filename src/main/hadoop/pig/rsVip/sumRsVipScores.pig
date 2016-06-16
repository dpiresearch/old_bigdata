-- 
-- This script will sum the scores over the a 90 period and group them by country, category, location,
-- and keyword
--
-- Input variables
--   inputdir - directory pointing to the input data
--   refdir - reference data (location and category human readable names
--   outputdir - directory to write output to
--
--

SET default_parallel 50;

D1 = LOAD '$inputdir/*/rs_scores/*' USING PigStorage() as (day:int, country:chararray, catid:int, locid:int, adid:long, kw:chararray,kwcount:long,score:float);

D11 = FOREACH D1 GENERATE *;
-- DUMP D11;

--
-- Generate the sum per country, cat, location, adid, keyword
--
D11G = GROUP D11 BY (country, catid, locid, adid, kw);
D11F = FOREACH D11G GENERATE group.country as country, group.catid as catid, group.locid as locid, group.adid as adid, group.kw as kw, SUM(D11.score) as score;

DESCRIBE D11F;
-- DUMP D11F;
STORE D11F INTO '$inputdir/$daydate/rs_sum';

