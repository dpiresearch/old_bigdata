--
-- this takes care of the latest (4) version of search results deployed 6/17/2013
--
-- This is used for testing individual test cases
--
-- - search matched with first view (uniqueness)
--

-- REGISTER '$deploydir/lib/pig_udf_ref.jar'
-- REGISTER '$deploydir/lib/piggybank.jar'

-- SET default_parallel 50;

--
-- LOAD data since the beginning of time - or however much data we have
--

F1 = LOAD '/user/hadoop/data/prod/ingest/search_results/$thedate/*/search_results.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,macid:chararray, clientid:chararray, catid:long, locid:long, searchstr:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray, listing_count:int,raw_term:chararray,zrpsearchstr:chararray);

F1G = FOREACH F1 GENERATE clientid, catid, locid, searchstr, referer, raw_term, zrpsearchstr;
F1G = FOREACH F1G GENERATE clientid, catid, locid, SUBSTRING(searchstr,0,INDEXOF(searchstr,'|',0)) as kw, SUBSTRING(searchstr,(int) INDEXOF(searchstr,'|',0),(int) SIZE(searchstr)) as none_str,referer, raw_term, SUBSTRING(zrpsearchstr,0,INDEXOF(zrpsearchstr,'|',0)) as zrpkw, SUBSTRING(zrpsearchstr,(int) INDEXOF(zrpsearchstr,'|',0),(int) SIZE(zrpsearchstr)) as zrp_str;

-- take out emtpy string
F1G = FILTER F1G BY (kw != 'EMPTY' AND kw != '');

-- sanity check
F1GL = LIMIT F1G 10;
-- DUMP F1GL;

--
-- START all search events --
--
F1G_ALL_SEARCH = GROUP F1G ALL;
F1G_ALL_SEARCHF = FOREACH F1G_ALL_SEARCH GENERATE COUNT(F1G);
-- DUMP F1G_ALL_SEARCHF;
STORE F1G_ALL_SEARCHF INTO '/user/hadoop/data/exp/all_search_count/$thedate';

-- END all search events --


--
-- START most popular search terms --
--
F1G_SEARCH_TERMS = GROUP F1G BY raw_term;
F1G_SEARCH_TERMS_F = FOREACH F1G_SEARCH_TERMS GENERATE group as kw, COUNT(F1G);
STORE F1G_SEARCH_TERMS_F INTO '/user/hadoop/data/exp/all_search_terms/$thedate';
--
-- END most popular search terms --
--

F1G = FILTER F1G BY (none_str == '|NONE');

--
-- START all search events with ZRP --
--
F1GN_ALL_SEARCH = GROUP F1G ALL;
F1GN_ALL_SEARCHF = FOREACH F1GN_ALL_SEARCH GENERATE COUNT(F1G);
-- DUMP F1GN_ALL_SEARCHF;
STORE F1GN_ALL_SEARCHF INTO '/user/hadoop/data/exp/zrp_search_count/$thedate';

--
-- END all search events with ZRP --
--

-- 
-- START Most frequent terms resulting in ZRP --
--
F1GG = GROUP F1G BY raw_term;
F1GGF = FOREACH F1GG GENERATE group as raw_term, COUNT(F1G) as kwcount;
O1 = ORDER F1GGF BY kwcount DESC;
O1L = LIMIT O1 100;
-- DUMP O1L;
STORE O1 INTO '/user/hadoop/data/exp/zrp_search_terms/$thedate';

--
-- END Most frequent terms resulting in ZRP --
--

ZRP1 = FILTER F1G BY (zrp_str == '|NONE');

--
-- START all search events with ZRP --
--
ZRP1_ALL_SEARCH = GROUP ZRP1 ALL;
ZRP1_ALL_SEARCHF = FOREACH ZRP1_ALL_SEARCH GENERATE COUNT(ZRP1);
-- DUMP ZRP1_ALL_SEARCHF;
STORE ZRP1_ALL_SEARCHF INTO '/user/hadoop/data/exp/zrp_zoomout_no_results_count/$thedate';

