--
--
-- BOLT-1729 : This script generates calculates latest available listing count per keyword.
-- Note: Since the keyword is searched under a particular location, category and the ad might
-- belong to a different location, category. The resulting output could show null search results
--   
--
-- Reads data generated by searchAllWords.pig
--
-- Data format:
--      Input:
--      1)      (site:int, cat:int, loc:int, kw:chararray, sc:int, sr:int)
--      2)      (site:int, cat:int, loc:int, kw:chararray, score:float)
--      Output:
--              (site:int, cat:int, loc:int, kw:chararray, score:float, sr:int)
--
-- Input Parameters:
-- searchResInputDir: Input dir where the aggregated search results are stored (output of searchAllWords.pig)
-- sumInputDir: Input dir where summed scores are stored (output of sumkeywords.pig)
-- theDate: Date folder name
-- outputDir: Output folder

-- SRD = LOAD '/user/data/grangaswamy/qa/keywords/{20130801,20130802,20130803,20130804,20130805,20130806,20130807}/search_keywords/*' using PigStorage() as (site:int, cat:int, loc:int, kw:chararray, sc:int, sr:int);
-- SRD = LOAD '/user/data/bolt/data/qa/daily/keywords/20130809/all_search_words/*' using PigStorage() as (sr_site:int, sr_cat:int, sr_loc:int, sr_kw:chararray, sc:int, sr:int);

SRD = LOAD '$searchResInputDir/$theDate/all_search_words/*' using PigStorage() as (sr_site:int, sr_cat:int, sr_loc:int, sr_kw:chararray, sc:int, sr:int);

DESCRIBE SRD;
-- SRD: {site: int,cat: int,loc: int,kw: chararray,sc: int,sr: int}

-- SCORES = LOAD '/user/data/bolt/data/qa/daily/popular_search/20130809/sum/par*' using PigStorage() as (site:int, cat:int, loc:int, kw:chararray, score:float);
SCORES = LOAD '$sumInputDir/$theDate/sum/par*' using PigStorage() as (site:int, cat:int, loc:int, kw:chararray, score:float);
DESCRIBE SCORES;

SCR_RES = JOIN SCORES BY (site, cat, loc, kw) LEFT OUTER, SRD BY (sr_site, sr_cat, sr_loc, sr_kw);
DESCRIBE SCR_RES;

SCR_RES_FILTER = FOREACH SCR_RES GENERATE site, cat, loc, kw, score, sc, sr;
DESCRIBE SCR_RES_FILTER;
STORE SCR_RES_FILTER INTO '$outputDir/$theDate/sum_sr';

-- Old stuff
-- SRG = GROUP SRD BY (site, cat, loc, kw);
-- DESCRIBE SRG;
-- SRG: {group: (site: int,cat: int,loc: int,kw: chararray),SRD: {(site: int,cat: int,loc: int,kw: chararray,sc: int,sr: int)}}
-- DUMP SRG;

-- SRA = FOREACH SRG GENERATE FLATTEN(group), (int)AVG(SRD.sr);
-- DESCRIBE SRA;
-- SRA: {group::site: int,group::cat: int,group::loc: int,group::kw: chararray,int}
-- DUMP SRA;