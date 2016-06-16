--
--
-- This script aggregates the active ads and groups them according to leaf category and L2 location.
-- 
-- Inputs:  $alldate - a list of dates (typically past thirty days) passed in via the script
--                     and formatted for pig to intepret.
--          $inputdir - input directory where search_results.log are found
--          $output   - output directory for results
--

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- $OUTLIMIT = TMPL;
}

-- some dir ending in data/prod/jobs/popular_search
S = LOAD '$inputdir/$thedate/clean/part*' USING PigStorage() as (country:chararray, catid:int, locid:int, kw:chararray, score:float, isTrending:boolean, trend_score:float);

SL = LIMITDUMP(S);
-- DUMP SL;

--
-- START LOAD REFERENCE DATA
--
-- Load the location file
LOC_FILE = LOAD '$refdir/$locfile' USING PigStorage(' ');

ZALOC = FOREACH LOC_FILE GENERATE (long)$0 as l1_loc, (long)$1 as l2_loc, (long)$2 as l3_loc, (long)$3 as l4_loc;

--
-- sanity check
--
Z1 = ALLCOUNT(ZALOC);
-- DUMP Z1;

-- Load the category file
CAT_FILE = LOAD '$refdir/$catfile' USING PigStorage(',');
ZACAT = FOREACH CAT_FILE GENERATE (long)$0 as site, (long)$1 as l1_cat, (long)$2 as l2_cat, (long)$3 as l3_cat, (long)$4 as l4_cat, (chararray)$5 as catname;

CC = LIMITDUMP(ZACAT);
-- DUMP CC;

--
-- END LOAD REFERENCE DATA
--

-- Join with the location reference information to produce a row for each level a leaf location is associated with
-- 
J1 = JOIN S BY locid, ZALOC BY l4_loc;
J1F = FOREACH J1 GENERATE S::country as country, S::kw as kw, S::catid as catid, S::locid as locid, ZALOC::l4_loc as l4, ZALOC::l3_loc as l3, ZALOC::l2_loc as l2, ZALOC::l1_loc as l1, S::score as score, S::isTrending as isTrending, S::trend_score as trend_score;

JL = LIMITDUMP(J1F);
-- DUMP JL;

-- Bag it to prepare for a flatten in order to explode out the rows
BJ = FOREACH J1F GENERATE country, kw, catid, ((l4 == l3)?TOBAG(l1, l2, l3):TOBAG(l1, l2, l3, l4)) as ll, score, trend_score;
DESCRIBE BJ;
BJL = LIMITDUMP(BJ);
-- DUMP BJL;

-- After the flatten and generate, you will have a score associated with each location level.   This will contribute to the total score.
BJEF = FOREACH BJ GENERATE country, kw, catid, score, trend_score, FLATTEN(ll);
BJEF1 = FOREACH BJEF GENERATE country, kw, catid, score, trend_score, $5 as locid;

-- DUMP BJEF;

--
-- Do the same thing for categories as for locations above
--
CJ = JOIN BJEF1 BY catid, ZACAT BY l4_cat;
CJF = FOREACH CJ GENERATE BJEF1::country as country, BJEF1::kw as kw, BJEF1::catid as catid, BJEF1::score as score, BJEF1::trend_score as trend_score, BJEF1::locid as locid, ZACAT::l1_cat as c1, ZACAT::l2_cat as c2, ZACAT::l3_cat as c3, ZACAT::l4_cat as c4;
-- CJF = FOREACH CJ GENERATE BJEF1::country as country, BJEF1::kw as kw, BJEF1::catid as catid, BJEF1::score as score, BJEF1::locid as locid, ZACAT::l1_cat as c1, ZACAT::l2_cat as c2, ZACAT::l3_cat as c3, ZACAT::l4_cat as c4;

CJFF = FOREACH CJF GENERATE country, kw, score, trend_score, locid, ((c4 == c3)?TOBAG(c1, c2, c3):TOBAG(c1, c2, c3, c4)) as cc;
DESCRIBE CJFF;

-- DUMP CJFF;

CJFFF = FOREACH CJFF GENERATE country, kw, score, trend_score, locid, FLATTEN(cc) as catid;
CJF3 = FOREACH CJFFF GENERATE country, kw, score, trend_score, locid, $5 as catid;
-- DUMP CJF3;

--
-- Sum everything up
--
ALLG = GROUP CJF3 BY (country, kw, locid, catid); 
ALLF = FOREACH ALLG GENERATE flatten(group), SUM(CJF3.score) as score, SUM(CJF3.trend_score) as trend_score;
ALLF = FOREACH ALLF GENERATE $0 as country, $3 as catid, $2 as locid, $1 as kw, score, ((trend_score > 0.0F)?'true':'false'), trend_score;
-- ALLF = FOREACH ALLG GENERATE flatten(group), SUM(CJF3.score);

-- DUMP ALLF;
STORE ALLF INTO '$outputdir/$thedate/aggsums';


