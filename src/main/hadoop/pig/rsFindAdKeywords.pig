--
-- Find keywords related to the ads
--
-- We read in the search-view output from the popular search search-view process
--
-- Assuming we're getting the following format:
-- country, searchtime, viewtime, cat id, loc id, keyword, findAdInId result, adid
--
-- Example:
-- 3       1379454634440   1379454642586   31      11834   ford    2437427/2437427 2437427
--

REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

F1 = LOAD '$inputdir/$daydate/sv_filtered/part*' USING PigStorage() as (site:long,searchtime:long, viewtime:long, catid:long,locid:long, kw:chararray, inSearch:chararray, adid:long);

-- F1G = FOREACH F1 GENERATE day as day, site, catid, locid, kw, kwcount;
F1G = FOREACH F1 GENERATE site, catid, locid, kw, adid;
-- DUMP F1G;


F1GG = GROUP F1G BY (site, catid, locid, adid, kw);
F1GGG = FOREACH F1GG GENERATE FLATTEN(group), COUNT(F1G) as kwcount; 

STORE F1GGG INTO '$outputdir/$daydate/rs_kwad';

F1GO = ORDER F1GGG by adid DESC, kwcount DESC;
FL = LIMIT F1GGG 10;
-- DUMP FL;

 
