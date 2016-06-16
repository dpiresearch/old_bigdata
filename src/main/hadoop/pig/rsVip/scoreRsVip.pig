-- 
-- This script will calculate a score for each country, category, location, adid, keyword tuple
-- based on the day passed in, which determines the decay factor applied to the count 
-- time passed in will be the "current" date
--
-- Params passed in:
--    day - which day is this designated as.  Used to get decay index
--    daydate - date corresponding to this day.  Used to read from the correct day directory
--    deploydir - deployment directory
--    inputdir - data input directory
-- 

REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

D1 = LOAD '$refdir/rs_decay.lst' USING PigStorage() as (day:int,factor:float);

F1 = LOAD '$inputdir/$daydate/rs_kwad/part*' USING PigStorage() as (country:chararray,catid:long,locid:long, adid:long, kw:chararray, kwcount:int);

-- country, catid, locid, adid, kw

F1G = FOREACH F1 GENERATE $day as day, country, catid, locid, adid, kw, kwcount;
-- DUMP F1G;

D1G = FOREACH D1 GENERATE day, factor;
-- DUMP D1G;

J1 = JOIN D1G by day, F1G by day;
J1F = FOREACH J1 GENERATE D1G::day as day, F1G::country as country, F1G::catid as catid, F1G::locid as locid, F1G::adid as adid, F1G::kw as kw, F1G::kwcount as kwcount, (F1G::kwcount * D1G::factor) as score;

DESCRIBE J1F;
-- DUMP J1F;
STORE J1F INTO '$outputdir/$daydate/rs_scores';

 
