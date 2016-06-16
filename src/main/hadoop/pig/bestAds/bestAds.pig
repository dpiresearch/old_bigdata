--
--      This pig script uses view ad logs (viewed_ad.log) as input
--      and aggregates top ad views for every L1 location id
--

A = LOAD '$inputdir/$alldate/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray,nativeref:int,country:chararray,lang:chararray);
-- A = LOAD '/user/data/bolt/data/qa/input/viewad/2013101*/*/*log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray,nativeref:int,country:chararray,lang:chararray);

-- category lookup
CLKP = LOAD '$refdir/categories.csv' USING PigStorage(',') as (site:long, cat0:long, cat1:long, cat2:long, cat3:long, cat4:long);
--CLKP = LOAD '/user/data/bolt/data/ref/categories.csv' USING PigStorage(',') as (site:long, cat1:long, cat2:long, cat3:long, cat4:long);

-- Calculcate ad views
A1 = FOREACH A GENERATE site, country, lang, catid, adid, usrid;
A1G = GROUP A1 BY (site, country, lang, catid, adid, usrid);
AV = FOREACH A1G GENERATE FLATTEN(group), COUNT(A1) as views;

-- Join with category lookup to fetch L1 Cat
AVJ = JOIN AV BY (site,catid), CLKP BY (site,cat4);
-- AVJ: {AV::group::site: long,AV::group::country: chararray,AV::group::lang: chararray,AV::group::catid: long,AV::group::adid: long,AV::group::usrid: long,AV::views: long,CLKP::site: long,CLKP::cat1: long,CLKP::cat2: long,CLKP::cat3: long,CLKP::cat4: long}

AVJ1 = FOREACH AVJ GENERATE AV::group::site, AV::group::country, AV::group::lang, CLKP::cat1, AV::group::catid, AV::group::adid, AV::group::usrid, AV::views;
-- AVJ1: {AV::group::site: long,AV::group::country: chararray,AV::group::lang: chararray,CLKP::cat1: long,AV::group::catid: long,AV::group::adid: long,AV::group::usrid: long,AV::views: long}

AVJ_S = ORDER AVJ1 BY site ASC, lang ASC, cat1 ASC, views DESC;

-- Limit 100 ads per group (country,lang,cat1)
AVJ_GRP = GROUP AVJ_S BY (site, country, lang, cat1);
AVJ_L = FOREACH AVJ_GRP { H = LIMIT AVJ_S 200 ; GENERATE FLATTEN(H);}

-- Order final output for readability
AVJ_L_ORD = ORDER AVJ_L BY site ASC, country ASC, lang ASC, cat1 ASC, views DESC;

-- DUMP AVJ_L_ORD;
STORE AVJ_L_ORD INTO '$outputdir/$thedate';