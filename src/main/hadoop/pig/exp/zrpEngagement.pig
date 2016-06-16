-- 
-- Find search results from zrp that were clicked
-- As of around mid October, the zrpstr captures ads that were pulled from solr 
-- when
--    - There are no search results and the zrp page is presented
--    - The SRP page has < <some_threshold> results, resulting in an additional module that shows more ads (again, stored in zrpstr)
--
--

REGISTER '$deploydir/lib/ad_search.jar'
REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- $OUTLIMIT = TMPL;
}

--
-- Modified to account for new fields referer country, language, and listing count
--
F1 = LOAD '$inputdir/search_results/$daydate/*/search_results.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,macid:chararray, clientid:chararray, catid:long, locid:long, searchstr:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray, listing_count:int, raw_kw:chararray, zrpstr:chararray);

V1 = LOAD '$inputdir/viewad/$daydate/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray);

-- Don't project ip yet
SV1 = FOREACH V1 GENERATE time, country, adid, macid, catid, locid;


G1 = GROUP F1 BY macid;
G1F = FOREACH G1 GENERATE flatten(group), COUNT(F1) as count;
G1FO = ORDER G1F BY count DESC;
G1FL = LIMITDUMP(G1FO);
-- DUMP G1FL;

G2 = GROUP V1 BY macid;
G2F = FOREACH G2 GENERATE flatten(group), COUNT(V1) as count;
G2FO = ORDER G2F BY count DESC;
G2FL = LIMITDUMP(G2FO);
-- DUMP G2FL;

-- 
-- START Section to tokenize searchstr
--

--
-- Project the keyword as well as the searchstr.  May want to project the keyword out later
--
CAT_LOC_TOK = FOREACH F1 GENERATE time, country, macid, catid, locid, clientid, SUBSTRING(searchstr,0,INDEXOF(searchstr,'|',0)) as kw, searchstr, nativereferer, zrpstr;


-- ***********************
-- START Filters for test
-- ***********************

CAT_LOC_TOK = FILTER CAT_LOC_TOK by (kw != 'EMPTY' AND kw != '');
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != 'EMPTY' AND macid != '' AND macid != 'UNKNOWN');
-- CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != '3b812842-2930-4c1e-93c4-ef5a7094432c');
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != 'be57e2a3-4e03-423d-b6e1-8d4cd8c15e77');
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != '3b812842-2930-4c1e-93c4-ef5a7094432c');
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != 'cb7df60b-47b7-4761-888b-a029c074f862');
-- Get only clicks from within the website

CAT_LOC_TOK = FILTER CAT_LOC_TOK by (nativereferer == 1);

CLTG = GROUP CAT_LOC_TOK ALL;
CLTGF = FOREACH CLTG GENERATE COUNT(CAT_LOC_TOK);
-- DUMP CLTGF;



--
-- Filter for only specific search terms
-- CAT_LOC_TOK = FILTER CAT_LOC_TOK by (kw == 'guitarra acustica');


-- DESCRIBE CAT_LOC_TOK;
-- CAT_LOC_TOK_L = LIMIT CAT_LOC_TOK 10;
-- DESCRIBE CLT_F;
-- CAT_LOC_TOK_L = LIMIT CLT_F 10;
-- DESCRIBE CAT_LOC_TOK_L;
-- DUMP CAT_LOC_TOK_L;

-- Filter out empty macids from the VIP clicks
SV1 = FILTER SV1 by (macid != 'EMPTY' AND macid != '' AND macid != 'UNKNOWN');

-- *********************
-- END Filters for test
-- *********************

--
-- To prevent the same view from being matched with multiple searches, 
-- project the timestamp of the view into the matched relation and make sure it's distinct.
--

-- 
-- To get search-view impressions within X minutes
-- project both timestamps to the relation and make sure viewtime - searchtime < X minutes
-- TODO:  Need to figure out if the referer on the view will be a better way
-- 
SVJ = JOIN SV1 by macid, CAT_LOC_TOK by macid USING 'skewed';

--
-- Make the search-view line unique by search time, keyword, and adid
--

--
-- Search for zrp ads that were clicked
--
SVJF = FOREACH SVJ GENERATE CAT_LOC_TOK::country as country, CAT_LOC_TOK::time as searchtime, SV1::time as viewtime, SV1::catid as view_catid, SV1::locid as view_locid, CAT_LOC_TOK::kw as kw, com.ebay.ecg.bolt.bigdata.pig.udf.FindAdidInSearch((chararray) SV1::adid, (chararray) CAT_LOC_TOK::zrpstr) as searchit, SV1::adid as adid;


-- filter out search-view pairs that don't match on adid
SVJFF = FILTER SVJF BY (searchit != 'false');

SVJFF_ALL = ALLCOUNT(SVJFF);
-- DUMP SVJFF_ALL;

-- filter for adview 30 minutes after search
SVJFF_30MIN = FILTER SVJFF by ((((long) viewtime - (long) searchtime) < 1800000) AND (((long) viewtime - (long) searchtime) > 0));


-- STORE SVJFF_30MIN INTO '$outputdir/$daydate/sv_filtered';

--
-- At this point we have have matched search and view ad within 30 minutes
--

SVJPROJ_30MIN = FOREACH SVJFF_30MIN GENERATE country, searchtime, view_catid, view_locid, kw;

SV_30 =  ALLCOUNT(SVJPROJ_30MIN);
-- DUMP SV_30;

SVJD = DISTINCT SVJPROJ_30MIN PARALLEL 50;

SVJ_GROUP_BY_KEYWORD = GROUP SVJD BY (country, view_catid, view_locid, kw) PARALLEL 50;
SVJ_KCOUNT = FOREACH SVJ_GROUP_BY_KEYWORD GENERATE group.$0 as country, group.$1 as catid, group.$2 as locid, group.$3 as kw, COUNT(SVJD) as kwcount;

DESCRIBE SVJ_KCOUNT;
-- DUMP SVJ_KCOUNT;
STORE SVJ_KCOUNT into '$outputdir/$daydate/zrp_kwcount_details';

SV_G = GROUP SVJ_KCOUNT ALL;
SV_GF = FOREACH SV_G GENERATE SUM(SVJ_KCOUNT.kwcount);
-- DUMP SV_GF;

STORE SV_GF into '$outputdir/$daydate/zrp_total';

