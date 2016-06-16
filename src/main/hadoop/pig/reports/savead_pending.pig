--
-- This report generates a list of field completion histograms of drafts
-- which are pending completion.
--
--
-- Invoked by runAdDraftHistReport.sh
--

REGISTER '$deploydir/lib/pig_udf_filterjson2.jar'
REGISTER '$deploydir/lib/piggybank.jar'

-- Load Drafts created on specific date
D1 = LOAD '/user/dapang/data/prod/save_ad/$thedate/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);
--D1 = LOAD '/user/dapang/data/prod/save_ad/20140911/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);

D2 = FILTER D1 BY state == 'C';
D3 = FOREACH D2 GENERATE id, time, site, state, locid, catid, com.ebay.ecg.bolt.bigdata.pig.udf.FilterJsonFieldNames((chararray)f1) as fields;
D3_FLAT = FOREACH D3 GENERATE id, time, site, state, locid, catid, flatten(fields);

-- Store Created Draft Ids per day
STORE D3_FLAT INTO '$outputdir/$thedate/created' USING PigStorage(',');

--------------------------------------------------------------
-- Load Drafts deleted
-- A1 = LOAD '/user/dapang/data/prod/save_ad/{20140911,20140912,20140913,20140914,20140915,20140916,20140917}/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);
A1 = LOAD '/user/dapang/data/prod/save_ad/{$daterange}/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);

DEL = FILTER A1 BY state == 'D';
DEL_F = FOREACH DEL GENERATE id, state; -- Has Dups !!

-- Remove duplicates
DEL_GG = GROUP DEL_F BY id;
DEL_GG_F = FOREACH DEL_GG GENERATE flatten(group) as delid;

-- Alt way of removing dups
DEL_G = GROUP DEL BY id;
DEL_G_LIM = FOREACH DEL_G {
        A = ORDER DEL BY time desc;
        B = LIMIT A 1;
        --B = LIMIT DEL 1;
        --GENERATE group, B;
        GENERATE group as id;
}
DESCRIBE DEL_G_LIM;

-- Completed Drafts
COMPLETED = JOIN D3_FLAT BY id, DEL_GG_F BY delid;

-- Store Completed Drafts
STORE COMPLETED INTO '$outputdir/$thedate/completed' USING PigStorage(',');

-- Created + Completed Drafts
DEL_J = JOIN D3_FLAT BY id LEFT OUTER, DEL_GG_F BY delid;
--DEL_J = JOIN D3_FLAT BY id LEFT OUTER, DEL_G_LIM BY id;


-- Drafts Pending Completion
MOD = FILTER DEL_J BY DEL_GG_F::delid is null;
--MOD = FILTER DEL_J BY DEL_G_LIM::id is null;

-- ==================================================
-- Find modified Drafts

MOD_ID = FOREACH MOD GENERATE D3_FLAT::id as modid;

M1 = FILTER A1 BY state == 'U';
M1_F = FOREACH M1 GENERATE id, time, site, state, locid, catid, com.ebay.ecg.bolt.bigdata.pig.udf.FilterJsonFieldNames((chararray)f1) as fields;
M1_FLAT = FOREACH M1_F GENERATE id, time, site, state, locid, catid, flatten(fields);

M1_GRP = GROUP M1_FLAT BY id;

UPD = FOREACH M1_GRP {
        A = ORDER M1_FLAT BY time DESC;
        B = LIMIT A 1;
        GENERATE group, flatten(B);
}
-- UPD: {group: chararray,B::id: chararray,B::time: long,B::site: chararray,B::state: chararray,B::locid: long,B::catid: long,B::fields::locationId: boolean,B::fields::categoryId: boolean,B::fields::Title: boolean,B::fields::Description: boolean,B::fields::ForSaleBy: boolean,B::fields::Phone: boolean,B::fields::UserName: boolean,B::fields::Price: boolean,B::fields::currencyValues: boolean,B::fields::Address: boolean

C_UPD_J = JOIN MOD_ID BY modid LEFT OUTER, UPD BY B::id;
-- C_UPD_J = JOIN MOD_ID BY modid, UPD BY B::id;


UPD_TRIM = FOREACH C_UPD_J GENERATE MOD_ID::modid, time, org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(time), site, state, locid, catid, locationId, categoryId, Title, Description, ForSaleBy, Phone, UserName, Price, currencyValues, Address;

-- Save Pending Data
STORE UPD_TRIM INTO '$outputdir/$thedate/pending' USING PigStorage(',');

-- ==================================================
-- Generate list of email ids of pending drafts

LOC_FILE = LOAD '$refdir/locations.csv' USING PigStorage(',') as (site:chararray, l0_loc:long, l1_loc:long, l2_loc:long, l3_loc:long, l4_loc:long, locname:chararray);
LOC_FILE = FILTER LOC_FILE BY site == 'MX';

CAT_FILE = LOAD '$refdir/categories.csv' USING PigStorage(',') as (siteid:chararray, l0_cat:long, l1_cat:long, l2_cat:long, l3_cat:long, l4_cat:long, catname:chararray);
CAT_FILE = FILTER CAT_FILE BY siteid == '3';

-- Fetch email id from created drafts
CR_EM = FOREACH D2 GENERATE id, com.ebay.ecg.bolt.bigdata.pig.udf.FilterEmailUsernameFromJson((chararray)f1) as fields;

-- Fetch draft ids from pending drafts
PND_ID = FOREACH UPD_TRIM GENERATE MOD_ID::modid as pending_id, locid, catid;

PND_EM = JOIN CR_EM BY id, PND_ID BY pending_id;
PND_EM_FLAT = FOREACH PND_EM GENERATE CR_EM::id as id, PND_ID::UPD::B::locid as locid, PND_ID::UPD::B::catid as catid, FLATTEN(CR_EM::fields);

J_CAT = JOIN PND_EM_FLAT BY catid LEFT OUTER, CAT_FILE by l4_cat;
J_CAT_F = FOREACH J_CAT GENERATE PND_EM_FLAT::id as id, PND_EM_FLAT::CR_EM::fields::email as email, PND_EM_FLAT::locid as locid, PND_EM_FLAT::catid as catid, CAT_FILE::catname as category;

J_CAT_LOC = JOIN J_CAT_F BY locid LEFT OUTER, LOC_FILE by l4_loc;

--EM = FOREACH J_CAT_LOC GENERATE J_CAT_F::id AS id, J_CAT_F::email as email, J_CAT_F::locid AS locid, LOC_FILE::locname AS location, J_CAT_F::catid AS catid, J_CAT_F::category AS category;
EM = FOREACH J_CAT_LOC GENERATE J_CAT_F::email as email, LOC_FILE::locname AS location, J_CAT_F::category AS category;

STORE EM INTO '$outputdir/$thedate/pending_emails' USING PigStorage(',');
--STORE EM INTO '/user/grangaswamy/email_report/pend_email_catloc' USING PigStorage(',');


-- ==================================================
CG = GROUP C_UPD_J ALL;
-- CG: {group: chararray,C_UPD_J: {(MOD_ID::modid: chararray,UPD::group: chararray,UPD::B::id: chararray,UPD::B::time: long,UPD::B::site: chararray,UPD::B::state: chararray,UPD::B::locid: long,UPD::B::catid: long,UPD::B::fields::locationId: int,UPD::B::fields::categoryId: int,UPD::B::fields::Title: int,UPD::B::fields::Description: int,UPD::B::fields::ForSaleBy: int,UPD::B::fields::Phone: int,UPD::B::fields::UserName: int,UPD::B::fields::Price: int,UPD::B::fields::currencyValues: int,UPD::B::fields::Address: int)}}

CG_COUNT = FOREACH CG GENERATE SUM(C_UPD_J.UPD::B::fields::locationId) as loc_count, SUM(C_UPD_J.UPD::B::fields::categoryId) as cat_count, SUM(C_UPD_J.UPD::B::fields::Title) as title_count, SUM(C_UPD_J.UPD::B::fields::Description) as desc_count, SUM(C_UPD_J.UPD::B::fields::Phone) as phone_count, SUM(C_UPD_J.UPD::B::fields::currencyValues) as val_count ;
DUMP CG_COUNT;

