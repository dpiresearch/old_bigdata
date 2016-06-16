-- 
-- This script will calculate a score for each country, category, location, keyword tuple
-- based on the day passed in, which determines the decay factor applied to the count 
-- time passed in will be the "current" date
--
-- Params passed in:
--    day - which day is this designated as.  Used to get decay index
--    daydate - date corresponding to this day.  Used to read from the correct day directory
-- 
--
-- TODO:  Need to pass in the full input and output path
--
--        Need script to sum everything up
--

REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

D1 = LOAD '$refdir/decay.lst' USING PigStorage(' ') as (day:int,factor:float);

F1 = LOAD '$inputdir/$daydate/searchview_kwcount/part*' USING PigStorage() as (country:chararray,catid:long,locid:long, kw:chararray, kwcount:int);

F1G = FOREACH F1 GENERATE $day as day, country, catid, locid, kw, kwcount;

-- Temporary line to fill in legacy logs that aggregated by siteid instead of country
-- Remove when we have enough logs (30 days)

F1G = FOREACH F1G GENERATE day, (country=='3'?'MX':country) as country, catid, locid, kw, kwcount;
F1G = FOREACH F1G GENERATE day, (country=='114'?'US':country) as country, catid, locid, kw, kwcount;
-- DUMP F1G;

D1G = FOREACH D1 GENERATE day, factor;
-- DUMP D1G;

J1 = JOIN D1G by day, F1G by day;
J1F = FOREACH J1 GENERATE D1G::day as day, F1G::country as country, F1G::catid as catid, F1G::locid as locid, F1G::kw as kw, F1G::kwcount as kwcount, (F1G::kwcount * D1G::factor) as score;

DESCRIBE J1F;
-- DUMP J1F;
STORE J1F INTO '$outputdir/$daydate/scores';

 
