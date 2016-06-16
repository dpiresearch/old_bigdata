--
--
-- test overlap function (part of Deduping investigation)
--
-- This script grabs all the keyword phrases searched by country, category, location
-- and calculates search result overlap
--
-- BOLT-3890
--
-- Expected input
-- 	inputdir - base directory for hdfs input
--      outputdir - base directory for hdfs output
-- 	alldate - all dates you want to use to determine the recent inventory 
-- 	thedate - date used to get the most recent popularity rating calculation
--

REGISTER '$deploydir/lib/pig_search_udf.jar';

-- set io.sort.mb 1000
set pig.exec.reducers.bytes.per.reducer 100000000

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- DESCRIBE $OUTLIMIT;
  -- $OUTLIMIT = TMPL;
}


-- ***************************************************************
-- START Load popularity rating from a file output from the databases
-- ***************************************************************

POPR = LOAD '$popsearch_inputdir/$thedate/sum/part*' using PigStorage() as (country:chararray, catid:chararray, locid:chararray, keyword:chararray, rating:float, isTrending:boolean, trend_score:float);

POPRF = FOREACH POPR GENERATE country as country, catid as catid, locid as locid, keyword as kw, rating as rating, isTrending as isTrending, trend_score as trend_score;

--
-- Sanity check
--
POPRFL = LIMITDUMP(POPRF);
DESCRIBE POPRFL;
-- DUMP POPRFL;

-- ***************************************************************
-- END Load popularity rating from a file output from the databases
-- ***************************************************************

--
-- ***************************************************************
-- Load search logs to get inventory
-- ***************************************************************
-- 
-- Load search results
-- We use the search_results log to get the most recent inventory numbers
-- We go back as much as possible to collect inventory numbers
-- 
A = LOAD '$searchlog_inputdir/$alldate/*/search_results.log*' using PigStorage('\u0001') as (version, time:long, site, macid, clientid, catid:chararray, locid:chararray, searchstr:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray, listing_count:int);

F = FOREACH A GENERATE time, country, macid, catid, locid, clientid, SUBSTRING(searchstr,0,INDEXOF(searchstr,'|',0)) as kw, searchstr, SUBSTRING(searchstr,INDEXOF(searchstr,'|',0),(int) SIZE(searchstr)) as resultstr;

-- remove empty keywords
FF = FILTER F by (kw != '' AND kw != 'EMPTY' AND kw != 'UNKNOWN');

-- remove empty or unknown users/clients
FF = FILTER FF by (macid != '' AND macid != 'UNKNOWN');

FFJ = FOREACH FF GENERATE country, catid, locid, kw, time, searchstr, resultstr;

--
-- Sanity check
--
FL = LIMITDUMP(FF);
DESCRIBE FL;
-- DUMP FL;

--
-- Get the max search time to get the most recent supply
--
G = GROUP FF BY (country, catid, locid, kw);
GF = FOREACH G GENERATE FLATTEN(group),  FLATTEN(FF.time) as time, MAX(FF.time) as maxtime;
GF = FILTER GF BY (time == maxtime);
GF = FOREACH GF GENERATE $0 as country, $1 as catid, $2 as locid, $3 as kw, time, maxtime;

--
-- Sanity check
--
GFL = LIMITDUMP(GF);
DESCRIBE GFL;
-- DUMP GFL;

-- ***************************************************************
-- JOIN to get the row that has the most recent inventory
-- ***************************************************************

J1 = JOIN GF BY (country, catid, locid, kw, maxtime), FFJ BY (country, catid, locid, kw, time);

J1G = FOREACH J1 GENERATE GF::country as country, GF::catid as catid, GF::locid as locid, GF::kw as kw, GF::maxtime as maxtime, FFJ::time as time, FFJ::resultstr as resultstr; 

--
-- Sanity check
--
J1GL = LIMITDUMP(J1G);
DESCRIBE J1GL;
-- DUMP J1GL;

--
-- J1G This gives you the most recent supply for a given (country, cat, loc, kw)
--

--
-- JOIN to get the popularity rating for all instances of (country, catid, locid, kw) that exist
-- in search results and in the precalculated sum for the past 30 days.
--

JPJ = JOIN J1G by (country, catid, locid, kw), POPRF by (country, catid, locid, kw);

JPJF = FOREACH JPJ GENERATE J1G::country as country, J1G::catid as catid, J1G::locid as locid, J1G::kw as kw, J1G::resultstr as resultstr, POPRF::rating as rating;

--
-- Sanity check
--
JPJFL = LIMITDUMP(JPJF);
-- DUMP JPJF;

-- Project a separate relation
--

JPJF2 = FOREACH JPJF GENERATE country, catid, locid, kw, resultstr, rating;

-- 
-- Once the filters are verified, the we can remove kw_* and resultstr*
--
-- cross join to compare keywords with each other
-- 
JALL = JOIN JPJF BY (country, catid, locid), JPJF2 by (country, catid, locid);

JALLF = FOREACH JALL GENERATE JPJF::country as country, JPJF::catid as catid, JPJF::locid as locid, JPJF::kw as kw_1, JPJF2::kw as kw_2, JPJF::rating as rating_1, JPJF2::rating as rating_2, JPJF::resultstr as r_1, JPJF2::resultstr as r_2;

-- Remove similar keywords
JALLF = FILTER JALLF BY (kw_1 != kw_2);

-- Remove redundant pairs by allowing only rating_1 > rating_2
JALLF = FILTER JALLF BY (rating_1 > rating_2);

JOL = FOREACH JALLF GENERATE country, catid, locid, kw_1, kw_2, rating_1, rating_2, (float) com.ebay.ecg.bolt.bigdata.pig.udf.FindSearchResultOverlap(r_1, r_2) as overlap;

-- STORE JOL INTO '/user/dapang/data/exp/overlap';

--
-- sanity check
JALLFL = LIMITDUMP(JOL);
--
-- DUMP JALLFL;

--
-- REJECTS = FILTER OUT ROWS that have OVERLAP > threshold
-- 
REJECTS = FILTER JOL BY (overlap > $threshold);

--
-- Do the grouping to remove multiple instances where
-- a keyword is > threshold when compared with several other keywords
-- in the same country, catid, locid
--
REJECTSG = GROUP REJECTS BY (country, catid, locid, kw_2);
-- REJECTSGL = LIMITDUMP(REJECTSG);
REJECTSGC = ALLCOUNT(REJECTSG);
-- DUMP REJECTSGC;
  
-- Here are the number of rejects
RD = ALLCOUNT(REJECTS);
-- DUMP RD;

RDD = LIMITDUMP(REJECTS);
DESCRIBE RDD;
-- DUMP RDD;

--
-- All the rejected keywords from the latest sum calculation go here
-- Can use this data to test overlap
--
STORE REJECTS INTO '$outputdir/$thedate/rejects';

-- Join rejects with sum output by country, category, location, keyword (keyword is the one with the lower popularity rating).

JT = JOIN REJECTS BY (country, catid, locid, kw_2) RIGHT OUTER, POPRF BY (country, catid, locid, kw);

JTF = FOREACH JT GENERATE POPRF::country as country, POPRF::catid as catid, POPRF::locid as locid, REJECTS::kw_2 as reject_kw, POPRF::kw as kw,
POPRF::rating as rating, POPRF::isTrending as isTrending, POPRF::trend_score as trend_score, REJECTS::overlap as overlap;


--
-- Sanity check store to see our last join
-- The schema is
--    country
--    catid
--    locid
--    kw_2     - rejected keywords.  Will be empty if there is no match
--    kw       - keywords from sum calculation
--    rating   - the popularity rating
--    isTrending - is the keyword trending?
--    trending_score - trending score
--    overlap        - overlap percentage.  We should see percentages > $threshold
--
-- STORE JTF INTO '$outputdir/$thedate/final_join';

--
-- If overlap is empty, that's because the row comes from POPRF and that doesn't have an overlap (not a match)
-- OUTPUT the rows that do not have a match (filters out the lower popularity keyword that have overlap > threshold
-- 
JTFALL = ALLCOUNT(JTF);
-- DUMP JTFALL;
JTFALLD = LIMITDUMP(JTF);
DESCRIBE JTFALLD;
-- DUMP JTFALLD;

--
--
JTFF = FILTER JTF BY (reject_kw is null);

JTFD = ALLCOUNT(JTFF);
-- DUMP JTFD;

JTFDD = LIMITDUMP(JTFF);
-- DUMP JTFDD;
-- JTFFO = ORDER JTFF BY country, catid, locid, overlap desc, rating;

JTFF_FINAL = FOREACH JTFF GENERATE country, catid, locid, kw, rating, isTrending, trend_score;

--
-- Modified final sum rows with the keywords not satisfying the threshold overlap removed
--
-- STORE JTFF INTO '$outputdir/$thedate/dedupe_sum';
STORE JTFF_FINAL INTO '$outputdir/$thedate/dedupe_sum';

--
-- List of rejected keywords
--
JTFFR = FILTER JTF BY (reject_kw is not null);
-- STORE JTFFR INTO '$outputdir/$thedate/rejected_after_join';

--
-- Check to see if we have repeat rows by country, catid, locid, reject_kw)
-- The count here should match the final sum, if not, then we generated more that our original 
-- calculated sum
--
JTFFG = GROUP JTFF BY (country, catid, locid, kw);
JTFFG_COUNT = ALLCOUNT(JTFFG);
-- DUMP JTFFG_COUNT;

JJG = GROUP JTFFR BY (country, catid, locid, reject_kw);
JJGL = LIMITDUMP(JJG);
-- DUMP JJGL;
