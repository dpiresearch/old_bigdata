--
--      This pig script uses view ad logs (viewed_ad.log) as input
--      and aggregates all view ads counts, regardless of referrer (search engine vs site)
--

SET default_parallel 8;

-- Load Log files
V1 = LOAD '$inputdir/*/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray);

-- Filter out adid and userid from log file
SV1 = FOREACH V1 GENERATE adid, usrid, 1 as count;

-- Group all similar adid & userid
SV1_G = GROUP SV1 BY (adid, usrid);

SV1_GF = FOREACH SV1_G GENERATE FLATTEN(group), SUM(SV1.count) as count;

-- Load baseline ViewAds (From Global Platform)
VGP = LOAD '$oldViews' USING PigStorage(',') as (adid:long, usrid:long, count:long);

VJ = JOIN SV1_GF BY (adid, usrid) LEFT OUTER, VGP BY (adid, usrid);

VJ_T = FOREACH VJ GENERATE SV1_GF::group::adid, SV1_GF::group::usrid, SV1_GF::count + (VGP::count is null ? 0 : VGP::count);

STORE VJ_T INTO '$outputdir/$thedate';