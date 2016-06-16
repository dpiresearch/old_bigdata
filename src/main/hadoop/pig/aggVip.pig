--
-- this takes care of the latest (4) version of search results deployed 6/17/2013
--
-- This is used for testing individual test cases
--
-- - search matched with first view (uniqueness)
--

REGISTER '$deploydir/lib/pig_udf_ref.jar'
REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

--
-- LOAD data since the beginning of time - or however much data we have
--

V1 = LOAD '$inputdir/*/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray,referer:chararray);

-- Project only what we need 
SV1 = FOREACH V1 GENERATE time, site, adid, usrid, macid, catid, locid, referer;

-- Take out unknown or empty referers
SV1 = FILTER SV1 BY (referer != 'UNKNOWN');

--
-- This version supports ibazar (MX/US) only.  Will use isNativeReferer after conversion via pig or when we
-- have collected enough logs after the Java logging utils has deployed
--
-- SV1_REFERER_FILTER = FOREACH SV1 GENERATE time, site, adid, usrid, macid, catid, locid, referer, (int) INDEXOF(referer,'ibazar',0) as isNative, SUBSTRING(referer,0,INDEXOF(referer,'/',10)) as domain;
SV1_REFERER_FILTER = FOREACH SV1 GENERATE time, site, adid, usrid, macid, catid, locid, (int) com.ebay.ecg.bolt.bigdata.pig.udf.IsExternalReferer((chararray) referer) as isExternal; 


-- Take only external referers
SV1_REFERER_FILTER_A = FILTER SV1_REFERER_FILTER by (isExternal == 1);

--
-- START Get the results we want - how many external hits did a VIP page get?
--
SV1_G = GROUP SV1_REFERER_FILTER_A BY (adid, usrid);
SV1_GF = FOREACH SV1_G GENERATE FLATTEN(group), COUNT(SV1_REFERER_FILTER_A) as hitCount;
DESCRIBE SV1_GF;
STORE SV1_GF INTO '$outputdir/$thedate/aggvip';
