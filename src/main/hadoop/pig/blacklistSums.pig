--
--
-- This script aggregates the active ads and groups them according to leaf category and L2 location.
-- 
-- Inputs:  $alldate - a list of dates (typically past thirty days) passed in via the script
--                     and formatted for pig to intepret.
--          $inputdir - input directory where search_results.log are found
--          $output   - output directory for results
--

REGISTER $deploydir/lib/pig_blacklist.jar;

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- $OUTLIMIT = TMPL;
}

-- some dir ending in data/prod/jobs/popular_search
S = LOAD '$inputdir/$thedate/sum/part*' USING PigStorage() as (country:chararray, catid:int, locid:int, kw:chararray, score:float, isTrending:boolean, trend_score:float);

-- S = LOAD '$inputdir/$thedate/sum/*' USING PigStorage() as (country:chararray, catid:int, locid:int, kw:chararray, score:float, isTrending:boolean, trend_score:float);

SL = LIMITDUMP(S);
-- DUMP SL;

-- F1 = FOREACH S GENERATE kw, 'SPACE', REGEX_EXTRACT_ALL(kw, '*luxury*') as match;
F1 = FOREACH S GENERATE country, catid, locid, kw, com.ebay.ecg.bolt.bigdata.pig.udf.IsBlacklisted(country,kw) as blacklist, score, isTrending, trend_score;
F1L = LIMITDUMP(F1);
-- DUMP F1L;

FF1 = FILTER F1 BY (blacklist == false);
FF1G = FOREACH FF1 GENERATE country, catid, locid, kw, score, isTrending, trend_score;
STORE FF1G INTO '$outputdir/$thedate/clean';

FB1 = FILTER F1 BY (blacklist == true);
FB1G = FOREACH FB1 GENERATE country, catid, locid, kw, score, isTrending, trend_score;
STORE FB1G INTO '$outputdir/$thedate/blacklisted';

