--
--

-- REGISTER '$deploydir/lib/pig_udf_ref.jar'
-- REGISTER '$deploydir/lib/piggybank.jar'

-- SET default_parallel 50;

--
-- LOAD data since the beginning of time - or however much data we have
--


F1 = LOAD '$inputdir/$thedate/*/reply_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:int,country:chararray,adId:long,catid:long, catName:chararray, locid:long,macid:chararray,clientIp:chararray,user_agent:chararray,modelAdId:chararray);

F1G = FOREACH F1 GENERATE country, catid;

-- sanity check
F1GL_SUCCESS = LIMIT F1G 10;
-- DUMP F1GL_SUCCESS;

F1GG = GROUP F1G BY (country, catid);
F1GGF = FOREACH F1GG GENERATE flatten(group), COUNT(F1G) as count;
F1GGF = FOREACH F1GGF GENERATE $0 as country, $1 as catid, count;

DESCRIBE F1GGF;
-- DUMP F1GGF;
STORE F1GGF INTO '$outputdir/$thedate/reply_ad_by_cat';  

