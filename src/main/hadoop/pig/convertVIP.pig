-- 
-- This script is used to update Version 3 View Ad logs to Version 4 
-- by adding the isNativeReferer flag, country, and language to the log
--
-- To be done when we roll out the PHX production cluster
--

REGISTER '/media/home/dapang/lib/piggybank.jar'

SET default_parallel 50;

--
-- LOAD data
--
V1 = LOAD '/user/data/bolt/data/viewad/20130826/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray);

-- Don't project ip yet
-- SV1 = FOREACH V1 GENERATE time, site, adid, macid, catid, locid, ip;
SV1 = FOREACH V1 GENERATE version, time, site, adid, macid, usrid, catid, locid, ip, ua, referer,0,(site==114?'US':'MX'),'es';

DESCRIBE SV1;
STORE SV1 INTO '/user/data/tmp/convertVIP' USING PigStorage('\u0001');


