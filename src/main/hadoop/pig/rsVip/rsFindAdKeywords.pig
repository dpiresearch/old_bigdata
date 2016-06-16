--
-- Find keywords related to the ads
--
-- We read in the search-view output from the popular search search-view process
-- and produce a kw adid grouping to eventually get keywords per adid
--
-- Assuming we're getting the following format:
-- country, searchtime, viewtime, cat id, loc id, keyword, findAdInId result, adid
--
-- Example:
-- MX       1379454634440   1379454642586   31      11834   ford    2437427/2437427 2437427
--

REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

F1 = LOAD '$inputdir/$daydate/sv_filtered/part*' USING PigStorage() as (country:chararray,searchtime:long, viewtime:long, catid:long,locid:long, kw:chararray, inSearch:chararray, adid:long);

-- Generate a unique id for search
F1G = FOREACH F1 GENERATE CONCAT(kw,(chararray) searchtime) as uniq_s, country, catid, locid, kw, adid;

F1GG = GROUP F1G BY uniq_s;

-- Get the topmost search-view match
SVJFFGF = FOREACH F1GG {
              result = TOP(1,1,F1G);
              GENERATE FLATTEN(result);
          }
DESCRIBE SVJFFGF;

S1 = FOREACH SVJFFGF GENERATE F1G::country as country, F1G::catid as catid, F1G::locid as locid, F1G::kw as kw, F1G::adid as adid; 

--
-- Get count per country, cat, loc, kw, adid
--
F1GG = GROUP S1 BY (country, catid, locid, adid, kw);
F1GGG = FOREACH F1GG GENERATE FLATTEN(group), COUNT(S1) as kwcount; 

STORE F1GGG INTO '$outputdir/$daydate/rs_kwad';


