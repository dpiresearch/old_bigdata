--
-- This report generates a list of drafts which are completed (Posted)
-- and the time it took to finish posting.
--
--
-- Invoked by runAdDraftHistReport.sh
--

-- C = LOAD '/user/dapang/data/prod/save_ad/20140520/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);
C = LOAD '/user/dapang/data/prod/save_ad/$thedate/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);

-- D = LOAD '/user/dapang/data/prod/save_ad/{20140520,20140521,20140522,20140523,20140524,20140525,20140526}/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);
D = LOAD '/user/dapang/data/prod/save_ad/{$daterange}/*/*' USING PigStorage('\u0001') as (version:int, time:long, site:chararray, lang:chararray, state:chararray, id:chararray, locid:long, catid:long, macid:chararray, ip:chararray, ua:chararray, f1:chararray);

CF = FILTER C BY state == 'C';
DF = FILTER D BY state == 'D';

DF_G = GROUP DF BY id;
DF_GG = FOREACH DF_G GENERATE flatten(group) as id, MAX(DF.time) as dTime;

COMP_GRP = COGROUP CF BY id, DF_GG BY id;
-- COMP_GRP: {group: chararray,CF: {(version: int,time: long,site: chararray,lang: chararray,state: chararray,id: chararray,locid: long,catid: long,macid: chararray,ip: chararray,ua: chararray,f1: chararray)},DF: {(version: int,time: long,site: chararray,lang: chararray,state: chararray,id: chararray,locid: long,catid: long,macid: chararray,ip: chararray,ua: chararray,f1: chararray)}}

-- Filter only the draft-post conversions
COMP_1 = FOREACH COMP_GRP GENERATE group, (IsEmpty(CF) ? null : CF.time) as X, (IsEmpty(DF_GG) ? null : DF_GG.dTime) as Y;
COMP_2 = FILTER COMP_1 BY X is not null AND Y is not null;

COMP_F = FOREACH COMP_2 GENERATE group, FLATTEN(X) as cTime, FLATTEN(Y) as dTime;

-- Find the post ad completion time in sec
COMP_TIME = FOREACH COMP_F GENERATE group as id, (dTime-cTime)/1000 as tdiff;
COMP_SORT = ORDER COMP_TIME BY tdiff asc;
DESCRIBE COMP_SORT;

--COMP_SORT_LIM = LIMIT COMP_SORT 100;
--DUMP COMP_SORT;

HIST_GRP = GROUP COMP_SORT BY tdiff;
DESCRIBE HIST_GRP;
HIST_CNT = FOREACH HIST_GRP GENERATE group as time, COUNT(COMP_SORT) as count;
DESCRIBE HIST_CNT;
--DUMP HIST_CNT;

STORE COMP_SORT INTO '$outputdir/$thedate/comp_time' USING PigStorage(',');
--STORE HIST_CNT INTO '/user/grangaswamy/drafts/time/20140520_26_hist' USING PigStorage(',');

-- Calculate Post Ad Clicks
P_CLK = FILTER C BY state == 'P';
P_CLK = FILTER P_CLK BY site == 'MX';
P_CLK_G = GROUP P_CLK ALL;
TOT = FOREACH P_CLK_G GENERATE COUNT(P_CLK);
DUMP TOT;

-- Group by MacId
MACS = GROUP P_CLK BY macid;
MAC_CLKS = FOREACH MACS GENERATE group as macid, COUNT(P_CLK) as clicks;
DESCRIBE MAC_CLKS;

STORE MAC_CLKS INTO '$outputdir/$thedate/post_clk' USING PigStorage(',');


-- Unique User Agents per Draft
C_DRAFTS = FILTER C BY state == 'C';
C_DRAFTS = FILTER C_DRAFTS BY ua != 'UNKNOWN';
C_DRAFTS_FILT = FOREACH C_DRAFTS GENERATE id, ua, time;

D_IDS = FOREACH C_DRAFTS GENERATE id as id; -- Draft ids of interest

D_FILT_ALL = FILTER D BY state != 'C';
D_FILT_ALL = FILTER D_FILT_ALL BY ua != 'UNKNOWN';

D_DRAFTS_ALL = FOREACH D_FILT_ALL GENERATE id, ua, time;
D_DRAFTS_J = JOIN D_IDS BY id, D_DRAFTS_ALL BY id; -- Filter out only Draft ids of interest
-- D_DRAFTS_J: {D_IDS::id: chararray,D_DRAFTS_ALL::id: chararray,D_DRAFTS_ALL::ua: chararray,D_DRAFTS_ALL::time: long}
D_DRAFTS = FOREACH D_DRAFTS_J GENERATE D_IDS::id as id, D_DRAFTS_ALL::ua as ua, D_DRAFTS_ALL::time as time;

D_DRAFTS_GRP = GROUP D_DRAFTS BY (id, ua);
D_DRAFTS_FLAT = FOREACH D_DRAFTS_GRP GENERATE FLATTEN(group), MAX(D_DRAFTS.time);

U_DRAFTS = UNION C_DRAFTS_FILT, D_DRAFTS_FLAT;
U_DRAFTS_G = GROUP U_DRAFTS BY (id, ua);
UA_DRAFTS = FOREACH U_DRAFTS_G GENERATE FLATTEN(group), MIN(U_DRAFTS.time);

UA_GRP = GROUP UA_DRAFTS BY id;
--UA_GRP: {group: chararray,UA_DRAFTS: {(group::id: chararray,group::ua: chararray,long)}}

UA_TRIM = FOREACH UA_GRP {
        FILT = FOREACH UA_DRAFTS GENERATE ua;
        GENERATE group as id , FILT;
    };
-- UA_TRIM: {id: chararray,FILT: {(group::ua: chararray)}}

UA = FOREACH UA_TRIM GENERATE id, FLATTEN(BagToTuple(FILT.group::ua));
STORE UA INTO '$outputdir/$thedate/draft_ua';


