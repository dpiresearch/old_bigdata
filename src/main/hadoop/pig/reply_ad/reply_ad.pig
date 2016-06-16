--
--
-- This script compiles all the replies for a particular date ($thedate)
-- also compiles detailed reply data for marketo and internal use
--
-- This is for BOLT-1471
--
-- refactored for BOLT-13118
--
--          $thedate  - the date to process, typically the day before
--          $inputdir - input directory where search_results.log are found
--          $output   - output directory for results
--

--
--
A = LOAD '$inputdir/tns_send_reply_ad/$thedate/*/tns_send_reply_ad.log*' USING PigStorage('\u0001') as (version, time, country, adid, clientid, replyemail, buyerphone, buyername, buyermsg, attachid, templatename,brandcode, replyId, replyDate, hardwareVendor, hardwareFamily, hardwareName, hardwareModel, platformVendor, platformName, platformVersion);

F = FOREACH A GENERATE time, country, clientid, (chararray) adid, replyemail, buyerphone, buyername, buyermsg,  attachid, templatename, brandcode, hardwareVendor, hardwareFamily, hardwareName, hardwareModel, platformVendor, platformName, platformVersion;

STORE F INTO '$outputdir/$thedate/fordwh';

--
-- Old obsolete reply logs 
-- START This section is used on the first day of deploy to reconcile new and old reply logs
--
-- A_OLD = LOAD '$inputdir/$thedate/*/reply_ad.log*' USING PigStorage('\u0001') as (version, time, site, country, adid, catid, catname, locid, macid,  clientid, useragent, referer, modeladid, sellerid, replyemail, buyerphone, buyername, buyermsg, attachid, templatename);

-- F_OLD = FOREACH A_OLD GENERATE time, country, clientid, adid, replyemail, buyerphone, buyername, buyermsg,  attachid, templatename;

-- STORE F_OLD INTO '$outputdir/$thedate/fordwh_old';

--
-- This is code to reconcile numbers between the old and new logs for the dwh on the day of deployment
-- Needed only temporarily
--
-- J1 = JOIN F BY (country, clientid, adid, replyemail) FULL OUTER, F_OLD BY (country, clientid, adid, replyemail);
-- J1F = FOREACH J1 GENERATE ((F_OLD::time is null)?F::time:F_OLD::time) as time, ((F_OLD::country is null)?F::country:F_OLD::country) as country, ((F_OLD::clientid is null)?F::clientid:F_OLD::clientid) as clientid, ((F_OLD::adid is null)?F::adid:F_OLD::adid) as adid, ((F_OLD::replyemail is null)?F::replyemail:F_OLD::replyemail) as replyemail, ((F_OLD::buyerphone is null)?F::buyerphone:F_OLD::buyerphone) as buyerphone, ((F_OLD::buyername is null)?F::buyername:F_OLD::buyername) as buyername, ((F_OLD::buyermsg is null)?F::buyermsg:F_OLD::buyermsg) as buyermsg, ((F_OLD::attachid is null)?F::attachid:F_OLD::attachid) as attachid, ((F_OLD::templatename is null)?F::templatename:F_OLD::templatename) as templatename, ((F::brandcode is null)?'':F::brandcode) as brandcode;

-- STORE J1F INTO '$outputdir/$thedate/fordwh_complete';
-- STORE J1F INTO '$outputdir/$thedate/fordwh';

--
-- END This section is used on the first day of deploy to reconcile new and old reply logs
-- 

--
-- START Marketo Reply object
--

--
-- Grab category and location explosion from the adObject processing
--
--
-- TODO - need to get replyid and replydate (from creation date)
--  Need CG1F
-- 

F1 = FOREACH A GENERATE time, country, clientid, (chararray) adid, replyemail, buyerphone, buyername, buyermsg,  attachid, templatename, brandcode, replyDate, replyId, '' as postMethod;

AD = LOAD '$jobdir/adObjects/*/ad_obj/*/part*' USING PigStorage('\t') as (email, creation, title, photo, phone, adThumb, currency, price, adUrl, endDate, l1_cat, l2_cat, l3_cat, l1_loc, l2_loc, l3_loc, l4_loc, t1, reply_count, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, adId);

AD1 = FOREACH AD GENERATE (chararray) adId as adId, l1_cat, l2_cat, l3_cat, l1_loc, l2_loc, l3_loc, l4_loc;

JAD = JOIN AD1 BY (adId), F1 BY (adid);
JADF = FOREACH JAD GENERATE F1::replyemail as email, AD1::adId as adId, AD1::l1_cat as l1_cat, AD1::l2_cat as l2_cat, AD1::l3_cat as l3_cat, AD1::l1_loc, AD1::l2_loc, AD1::l3_loc, AD1::l4_loc, F1::postMethod as postMethod, F1::replyDate as replyDate, F1::replyId as replyId, F1::postMethod;

-- DUMP JADF;
STORE JADF INTO '$jobdir/replyObjects/$thedate/replies';
