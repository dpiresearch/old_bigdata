--
--      This pig script uses view ad logs (viewed_ad.log) as input
--      and aggregates all view ads counts, regardless of referrer (search engine vs site)
--

SET default_parallel 8;

V1 = LOAD '$inputdir/*/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray);
--V1 = LOAD '/user/data/bolt/data/qa/input/viewad/*/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray);

-- /user/data/bolt/data/qa/input/viewad/20130903/1/viewed_ad.log.Tue
-- V1 = LOAD '$inputdir/*/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray);

-- Filter out adid and userid from log file
SV1 = FOREACH V1 GENERATE adid, usrid;

-- Group all similar adid & userid
SV1_G = GROUP SV1 BY (adid, usrid);

SV1_GF = FOREACH SV1_G GENERATE FLATTEN(group), COUNT(SV1);

STORE SV1_GF INTO '$outputdir/$thedate';
-- STORE SV1_GF INTO '/user/data/bolt/data/qa/vipVisitCount/20130903';