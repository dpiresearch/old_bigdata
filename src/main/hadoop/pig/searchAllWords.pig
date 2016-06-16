--
--
-- This script aggregates the counts for each occurance of the keyword along the (country, category, location) dimensions.
-- This is for BOLT-1471
--
-- Inputs:  $alldate - a list of dates (typically past thirty days) passed in via the script
--                     and formatted for pig to intepret.
--          $inputdir - input directory where search_results.log are found
--          $output   - output directory for results
--

A = LOAD '$inputdir/$alldate/*/search_results.log*' using PigStorage('\u0001') as (version, time, site, macid, clientid, catid, locid, searchstr:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray, listing_count:int);

F = FOREACH A GENERATE time, country, macid, catid, locid, clientid, SUBSTRING(searchstr,0,INDEXOF(searchstr,'|',0)) as kw, listing_count;
FF = FILTER F by (kw != '' AND kw != 'EMPTY' AND kw != 'UNKNOWN');
FF = FILTER FF by (macid != '' AND macid != 'UNKNOWN');

FL = LIMIT FF 10;
DESCRIBE FL;
-- DUMP FL;

G = GROUP FF BY (country, catid, locid, kw);
-- GF = FOREACH G GENERATE FLATTEN(group), COUNT(FF), (int)MIN(FF.listing_count) as listing_total;
GF = FOREACH G GENERATE FLATTEN(group), COUNT(FF), (int)MAX(FF.listing_count) as listing_total;
STORE GF INTO '$outputdir/$thedate/all_search_words'; 

--
-- Following gets the most recent listing_total vs just getting the MIN(listing_total)
-- Commented out for now since it hasn't been integrated with the above GF relation
--
-- MAX =  GENERATE the max timestamp for each country, catid, locid, kw
--
-- GMAX = FOREACH G GENERATE FLATTEN(group), MAX(FF.time) as maxtime, (int)MIN(FF.listing_count) as listing_total;
-- GMAX = FOREACH GMAX GENERATE $0 as country, $1 as catid, $2 as locid, $3 as kw, maxtime, listing_total;

-- Sanity check
-- GMAXL = LIMIT GMAX 10;
-- DUMP GMAXL;

--
-- JOIN to match the timestamps to get one row out of F, which holds the most recent listing_total
--
-- JGM = JOIN GMAX BY (country, catid, locid, kw, maxtime), F BY (country, catid, locid, kw, time);
-- JGMF = FOREACH JGM GENERATE GMAX::maxtime as time, F::country as country, F::catid as catid, F::locid as locid, F::kw as kw, F::listing_count as listing_count, GMAX::listing_total as listing_total;

-- Sanity check
-- JGMFG = GROUP JGMF ALL;
--  JGMFGC = FOREACH JGMFG GENERATE COUNT(JGMF);
-- DUMP JGMFGC;

-- Sanity check
-- JGMFL = LIMIT JGMF 10;
-- DUMP JGMFL;

-- Now latest listing count for each country, catid, locid, kw in JGMF

--
-- JOIN BACK TO G BY (country, catid, locid, kw)
--
