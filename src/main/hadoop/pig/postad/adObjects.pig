--
--
-- This creates the adObject for the Marketo service
-- 
-- Expects the following parameters:
--   inputdir - generalized inputdir
--   outputdir - where results are sent
--   deploydir - where to find libraries
--   refdir - where to find reference data (categories and locations)
--   thedate - which date are we processing data for
--  -param inputdir=/user/hadoop/data/pp/ingest -param outputdir=/user/hadoop/data/pp/jobs/adObjects -param deploydir=/home/hadoop/current -param refdir=/user/hadoop/data/pp/ref
--
--
--
-- TODO items
--   split country specific processing
--     each country will have it's own categories and locations naming files
--     process post and reply data separate by country, so each will join with their country specific name files
--   then combine them together at the end
--
REGISTER '$deploydir/lib/paramMap.jar'
REGISTER '$deploydir/lib/topN.jar'

--
-- LOAD data since the beginning of time - or however much data we have
--

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- $OUTLIMIT = TMPL;
}


F1 = LOAD '$inputdir/post_ad/$thedate/all/post_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,macid_noop:chararray, clientid:chararray,username:chararray,macid:chararray,catid:long, locid:long, user_agent:chararray, country:chararray, language:chararray, field_errors:chararray, attr_errors:chararray, other_errors:chararray, referer:chararray, paramMap:chararray, anonUsrId:chararray, cookies:chararray, adUrl:chararray, adId:chararray,creation:chararray,endDate:chararray, currency:chararray);

F1G = FOREACH F1 GENERATE adId, country, language, catid, locid, adUrl, creation, endDate, referer, currency, com.ebay.ecg.bolt.bigdata.pig.udf.RequestParamToBag(paramMap) as pMap;

-- F1G = FILTER F1G BY (country == 'ZA');
-- F1G = FILTER F1G BY (adId == '121837266');
DESCRIBE F1G;
F1GL = LIMITDUMP(F1G);
-- DUMP F1GL;
F1GLF = FOREACH F1G GENERATE (chararray) adId, country as country, language, catid, locid, adUrl, creation, endDate, currency, flatten(pMap);
DESCRIBE F1GLF;
-- DUMP F1GLF;

--
-- Reproject the fields after the flatten
--
F2 = FOREACH F1GLF GENERATE (chararray) $0 as adId, (chararray) $1 as country, (chararray) $2 as language, (chararray) $3 as catid, (chararray) $4 as locid, (chararray) $5 as adUrl, (chararray) $6 as creation, (chararray) $7 as endDate, (chararray) $8 as currency, (chararray) $9 as key, (chararray) $10 as value;

DESCRIBE F2;
--
-- "Filter" out the fields we're interested in from the param map
--
TITLE1 = FILTER F2 BY (key == 'Title');
PRICE1 = FILTER F2 BY (key == 'Price');
EMAIL1 = FILTER F2 BY (key == 'Email');
DESCRIBE EMAIL1;
PHOTO1 = FILTER F2 BY (key == 'pictures');
THUMB1 = FILTER F2 BY (key == 'picturesThumb');
PHONE1 = FILTER F2 BY (key == 'Phone');
DESCRIBE PHONE1;
P1L = LIMITDUMP(PHONE1);
-- DUMP P1L;
--
-- Cogroup to get all the fields in one row
--
CG1 = COGROUP TITLE1 BY (adId), PRICE1 BY (adId), EMAIL1 BY (adId), PHOTO1 BY (adId), THUMB1 BY (adId), PHONE1 BY (adId);
-- DUMP CG1;

CG1F = FOREACH CG1 GENERATE flatten(group), TITLE1.catid as catid, TITLE1.locid as locid, TITLE1.adUrl as adUrl, TITLE1.creation as creation, TITLE1.endDate as endDate, TITLE1.value as title, TITLE1.currency as currency, PRICE1.value as price, EMAIL1.value as email, PHOTO1.value as photo, THUMB1.value as adThumb, PHONE1.value as phone;
-- 
-- DUMP CG1F;

CG1F = FOREACH CG1F GENERATE $0 as adId,com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(catid,1) as catid,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(locid,1) as locid,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(adUrl,1) as adUrl,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(creation,1) as creation,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(endDate,1) as endDate,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(title,1) as title,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(currency,1) as currency,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(price,1) as price,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(email,1) as email,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(photo,1) as photo,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(adThumb,1) as adThumb,
com.ebay.ecg.bolt.bigdata.pig.udf.TopNFromBag(phone,1) as phone;
 
-- 
-- TODO - figure out how to include empty bags
-- 
-- This works, but produces a {()}
-- CG1F = FOREACH CG1F GENERATE $0 as adId, flatten(title), flatten(price), flatten(email), (IsEmpty(photo)?TOBAG(''):photo);
-- This works - bincond for email has BAGS for true and false conditions
-- CG1F = FOREACH CG1F GENERATE $0 as adId, flatten(title), flatten(price), flatten(email), (IsEmpty(email)?TOBAG(''):email),(IsEmpty(photo)?'false':'true'), (IsEmpty(adThumb)?'false':'true');

-- CG1F = FOREACH CG1F GENERATE $0 as adId, flatten(currency) as currency, flatten(catid) as catid, flatten(locid) as locid, flatten(adUrl) as adUrl, flatten(creation) as creation, flatten(endDate) as endDate, flatten(title) as title, flatten(price) as price, flatten(email) as email, (IsEmpty(photo)?'false':'true') as photo, (IsEmpty(adThumb)?'false':'true') as adThumb, (IsEmpty(phone)?'false':'true') as phone;

--
-- Test for new code
--
CG1F = FOREACH CG1F GENERATE $0 as adId, flatten(currency) as currency, flatten(catid) as catid, flatten(locid) as locid, flatten(adUrl) as adUrl, flatten(creation) as creation, flatten(endDate) as endDate, flatten(title) as title, flatten(price) as price, flatten(email) as email, ((photo is null)?'false':'true') as photo, ((adThumb is null)?'false':'true') as adThumb, ((phone is null)?'false':'true') as phone;

-- DUMP CG1F;
--
-- test catid
CAT1 = FOREACH CG1F GENERATE adId, catid, locid;
-- DUMP CAT1;
--
-- START South Africa Section
--
-- expand the South Africa location and categories
--

CAT_FILE = LOAD '$refdir/catnames/za/part*' USING PigStorage();
ZACAT = FOREACH CAT_FILE GENERATE (long)$0 as l4_cat, (chararray)$1 as l1_catname, (chararray)$2 as l2_catname, (chararray)$3 as l3_catname, (chararray)$4 as l4_catname;

-- DUMP CAT_FILE;

LOC_FILE = LOAD '$refdir/locnames/za/part*' USING PigStorage();
ZALOC = FOREACH LOC_FILE GENERATE (long)$0 as l4_loc, (chararray)$1 as l1_locname, (chararray)$2 as l2_locname, (chararray)$3 as l3_locname, (chararray) $4 as l4_locname;

CAT1 = JOIN CG1F BY (catid), ZACAT BY (l4_cat);
CG2F = FOREACH CAT1 GENERATE CG1F::adId as adId, CG1F::catid as catid, ZACAT::l1_catname as l1_cat, ZACAT::l2_catname as l2_cat, ZACAT::l3_catname as l3_cat, CG1F::locid as locid, CG1F::adUrl as adUrl, CG1F::creation as creation, CG1F::endDate as endDate, CG1F::title as title, CG1F::price as price, CG1F::email as email, CG1F::photo as photo, CG1F::adThumb as adThumb, CG1F::phone as phone, CG1F::currency as currency;

CG2FL = LIMITDUMP(CG2F);
-- DUMP CG2FL;

LOCCAT1 = JOIN CG2F BY (locid), ZALOC BY (l4_loc);
LCF = FOREACH LOCCAT1 GENERATE CG2F::adId as adId, CG2F::l1_cat as l1_cat, CG2F::l2_cat as l2_cat, CG2F::l3_cat as l3_cat, CG2F::locid as locid, ZALOC::l1_locname as l1_loc, ZALOC::l2_locname as l2_loc, ZALOC::l3_locname as l3_loc, ZALOC::l4_locname as l4_loc, CG2F::adUrl as adUrl, CG2F::creation as creation, CG2F::endDate as endDate, CG2F::title as title, CG2F::price as price, CG2F::email as email, CG2F::photo as photo, CG2F::adThumb as adThumb, CG2F::phone as phone, CG2F::currency as currency;

-- DUMP LCF;

-- 
-- get reply data
-- 
-- from the old reply log
-- 
-- R = LOAD '$inputdir/reply_ad/$thedate/all/reply_ad.log*' USING PigStorage('\u0001') as (version, time, site, country, adid, catid, catname, locid, macid,  clientid, useragent, referer, modeladid, sellerid, replyemail, buyerphone, buyername, buyermsg, attachid, templatename);

--
-- from the batch created tnsSendJobRunner log
-- We only want the count of adIds, don't need to grab all the fields
--
-- R = LOAD '$inputdir/tns_send_reply_ad/$thedate/all/tns_send_reply_ad.log*' USING PigStorage('\u0001') as (version, time, country, adid);
R = LOAD '$inputdir/tns_send_reply_ad/*/all/tns_send_reply_ad.log*' USING PigStorage('\u0001') as (version, time, country, adid);

RG = GROUP R BY (adid);
RGF = FOREACH RG GENERATE flatten(group) as adid, COUNT(R) as reply_count;

-- DUMP RGF;

--
-- Join reply data by adid
--
RGJ = JOIN LCF BY (adId) LEFT OUTER, RGF BY (adid);

RGJF = FOREACH RGJ GENERATE LCF::adId as adId, LCF::l1_cat as l1_cat, LCF::l2_cat as l2_cat, LCF::l3_cat as l3_cat, LCF::locid as locid, LCF::l1_loc, LCF::l2_loc, LCF::l3_loc, LCF::l4_loc, LCF::adUrl as adUrl, LCF::creation as creation, LCF::endDate as endDate, LCF::title as title, LCF::price as price, LCF::email as email, LCF::photo as photo, LCF::adThumb as adThumb, (RGF::reply_count IS NULL?0L:RGF::reply_count) as reply_count, LCF::phone as phone, LCF::currency as currency;

-- Dump ads that have replies
-- DUMP RGJF;
RGJFF = FILTER RGJF BY (reply_count > 0L);
-- 
-- Enabled by default
--
STORE RGJFF INTO '$outputdir/adObjects/$thedate/reply_count/za';

--
-- relation for the final format for ad object
--
RGJF_FINAL = FOREACH RGJF GENERATE email, creation, title, photo, phone, adThumb, currency, price, adUrl, endDate, l1_cat, l2_cat, l3_cat, l1_loc, l2_loc, l3_loc, l4_loc, '', reply_count, '', '', '', '', '', '', '', '', '', '', '', '', '', adId, '', '', '', '', '', '', '', '', '', '', '', ''; 
-- DUMP RGJF_FINAL;
--
-- Enabled by default
--
STORE RGJF_FINAL INTO '$outputdir/adObjects/$thedate/ad_obj/za';

-- STOP South Africa Section

-- START Ireland Section
--
-- expand the Ireland location and categories
--

IE_CAT_FILE = LOAD '$refdir/catnames/ie/part*' USING PigStorage();
IECAT = FOREACH IE_CAT_FILE GENERATE (long)$0 as l4_cat, (chararray)$1 as l1_catname, (chararray)$2 as l2_catname, (chararray)$3 as l3_catname, (chararray)$4 as l4_catname; 

IE_CAT1 = JOIN CG1F BY (catid), IECAT BY (l4_cat);
CG2F = FOREACH IE_CAT1 GENERATE CG1F::adId as adId, CG1F::catid as catid, IECAT::l1_catname as l1_cat, IECAT::l2_catname as l2_cat, IECAT::l3_catname as l3_cat, CG1F::locid as locid, CG1F::adUrl as adUrl, CG1F::creation as creation, CG1F::endDate as endDate, CG1F::title as title, CG1F::price as price, CG1F::email as email, CG1F::photo as photo, CG1F::adThumb as adThumb, CG1F::phone as phone, CG1F::currency as currency;

CG2FL = LIMITDUMP(CG2F);
-- DUMP CG2FL;

IE_LOC_FILE = LOAD '$refdir/locnames/ie/part*' USING PigStorage();
IELOC = FOREACH IE_LOC_FILE GENERATE (long)$0 as l4_loc, (chararray)$1 as l1_locname, (chararray)$2 as l2_locname, (chararray)$3 as l3_locname, (chararray) $4 as l4_locname;

IE_LOCCAT1 = JOIN CG2F BY (locid), IELOC BY (l4_loc);
IE_LCF = FOREACH IE_LOCCAT1 GENERATE CG2F::adId as adId, CG2F::l1_cat as l1_cat, CG2F::l2_cat as l2_cat, CG2F::l3_cat as l3_cat, CG2F::locid as locid, IELOC::l1_locname as l1_loc, IELOC::l2_locname as l2_loc, IELOC::l3_locname as l3_loc, IELOC::l4_locname as l4_loc, CG2F::adUrl as adUrl, CG2F::creation as creation, CG2F::endDate as endDate, CG2F::title as title, CG2F::price as price, CG2F::email as email, CG2F::photo as photo, CG2F::adThumb as adThumb, CG2F::phone as phone, CG2F::currency as currency;

-- DUMP IE_LCF;

--
-- Join reply data by adid
--
IE_RGJ = JOIN IE_LCF BY (adId) LEFT OUTER, RGF BY (adid);

IE_RGJF = FOREACH IE_RGJ GENERATE IE_LCF::adId as adId, IE_LCF::l1_cat as l1_cat, IE_LCF::l2_cat as l2_cat, IE_LCF::l3_cat as l3_cat, IE_LCF::locid as locid, IE_LCF::l1_loc, IE_LCF::l2_loc, IE_LCF::l3_loc, IE_LCF::l4_loc, IE_LCF::adUrl as adUrl, IE_LCF::creation as creation, IE_LCF::endDate as endDate, IE_LCF::title as title, IE_LCF::price as price, IE_LCF::email as email, IE_LCF::photo as photo, IE_LCF::adThumb as adThumb, (RGF::reply_count IS NULL?0L:RGF::reply_count) as reply_count, IE_LCF::phone as phone, IE_LCF::currency as currency;

-- Dump ads that have replies
-- DUMP RGJF;
IE_RGJFF = FILTER IE_RGJF BY (reply_count > 0L);
--
-- Enabled by default
--
STORE IE_RGJFF INTO '$outputdir/adObjects/$thedate/reply_count/ie';

--
-- relation for the final format for ad object
--
IE_RGJF_FINAL = FOREACH IE_RGJF GENERATE email, creation, title, photo, phone, adThumb, currency, price, adUrl, endDate, l1_cat, l2_cat, l3_cat, l1_loc, l2_loc, l3_loc, l4_loc, '', reply_count, '', '', '', '', '', '', '', '', '', '', '', '', '', adId, '', '', '', '', '', '', '', '', '', '', '', ''; 
-- DUMP IE_RGJF_FINAL;
-- Enable by default
STORE IE_RGJF_FINAL INTO '$outputdir/adObjects/$thedate/ad_obj/ie';

-- END Ireland Section

