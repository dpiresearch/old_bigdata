--
--
-- This script aggregates the active ads and groups them according to leaf category and L2 location.
-- 
-- Inputs:  $alldate - a list of dates (typically past thirty days) passed in via the script
--                     and formatted for pig to intepret.
--          $inputdir - input directory where search_results.log are found
--          $output   - output directory for results
--

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- $OUTLIMIT = TMPL;
}

--
-- LOAD THE DB snapshot
--
-- DB = LOAD '/user/hadoop/data/exp/ad_events/20140201/*_shard_*.csv' using PigStorage() as (adid:long, time:long, start_date:long, category_id:long, location_id:long,ad_state:chararray);
DB = LOAD '$inputdir/$dumpdate/*_shard_*.csv' using PigStorage() as (adid:long, time:long, start_date:long, category_id:long, location_id:long,ad_state:chararray);

DBF = FOREACH DB GENERATE adid, time, start_date, category_id, location_id, ad_state;

--
-- LOAD logs
--
A = LOAD '$inputdir/*/*/ad_event.log*' using PigStorage('\u0001') as (version, time:long, solraction:int, adid:long, start_date:long, creation_date:long, modification_date:long, last_user_date:long, location_path:chararray, category_id:long, location_id:long, ad_state:chararray, ad_state_detail:chararray);

F = FOREACH A GENERATE adid, time, start_date, category_id, location_id, ad_state;

F = FILTER F BY (ad_state == 'ACTIVE' OR ad_state == 'DELETED');

-- FAC = ALLCOUNT(F);
FAC = LIMITDUMP(F);
-- DUMP FAC;

FALL = UNION F, DBF;

--
-- For now, do the union here.  In the future, can probably group by the db data separately and union it with the
-- ad_events.log grouped by data below
--

P = ALLCOUNT(FALL);
-- DUMP P;

--
-- Only active or deleted ads
--
FF = FILTER FALL by (ad_state == 'ACTIVE' OR ad_state == 'DELETED');

--
-- Get one row per category location, ad_state, and get the most recent start_date
--
FG = GROUP FF BY (adid, category_id, location_id, ad_state);
FGF = FOREACH FG GENERATE flatten(group), COUNT(FF) as count, MAX(FF.start_date) as start_date;

-- order by count
FGFO = ORDER FGF BY count desc;
DESCRIBE FGFO;
-- DUMP FGFO;

-- get the max start_date
FGFF = FOREACH FGF GENERATE $0 as adid, $1 as category_id, $2 as location_id, $3 as ad_state, (long)start_date;
DESCRIBE FGFF;
-- DUMP FGFF;

FA = FILTER FGFF by (ad_state == 'ACTIVE');
FL = LIMIT FA 10;
DESCRIBE FL;
-- DUMP FL;

FD = FILTER FGFF by (ad_state == 'DELETED');
FL = LIMIT FD 10;
DESCRIBE FL;
-- DUMP FL;

-- LOC_FILE = LOAD '/user/hadoop/data/prod/ref/za_loc.csv' USING PigStorage(' ');
LOC_FILE = LOAD '$refdir/$locfile' USING PigStorage(' ');

ZALOC = FOREACH LOC_FILE GENERATE (long)$0 as l1_loc, (long)$1 as l2_loc, (long)$2 as l3_loc, (long)$3 as l4_loc;

-- figure out the parent
-- by comparing l3 and l4
-- if they are equal, the use l2 as the parent
-- generate the new parent/child list
--

--
-- sanity check
--
Z1 = ALLCOUNT(ZALOC);
-- DUMP Z1;

-- 
-- Remove ads that are in deleted state
-- 

J1 = JOIN FA BY (adid) LEFT OUTER, FD BY (adid);
J2 = FOREACH J1 GENERATE FA::adid as adid, FA::ad_state as active_state, FD::ad_state as deleted_state, FA::start_date as start_date, FA::category_id as category_id, FA::location_id as location_id;
JL = FILTER J2 BY (deleted_state is null);

--
-- Get L2 Location
--
JLJ = JOIN JL BY (location_id) LEFT OUTER, ZALOC BY (l4_loc);
-- JLJF = FOREACH JLJ GENERATE JL::adid as adid, JL::active_state as active_state, JL::start_date as start_date, JL::category_id as category_id, (ZALOC::l3_loc == ZALOC::l4_loc?ZALOC::l2_loc:ZALOC::l3_loc) as location_id, JL::location_id as orig_location_id;
JLJF = FOREACH JLJ GENERATE JL::adid as adid, JL::active_state as active_state, JL::start_date as start_date, JL::category_id as category_id,(ZALOC::l3_loc == ZALOC::l4_loc?ZALOC::l2_loc:ZALOC::l3_loc) as location_id, JL::location_id as orig_location_id;


--
-- we now have the the active ads with l2 locations
JLFJ_DUMP = ALLCOUNT(JLJF);
-- DUMP JLFJ_DUMP;

JLFJLD = LIMITDUMP(JLJF);
-- DUMP JLFJLD;

JLFJ_ACTIVE_LOC = FILTER JLJF BY (location_id is not null);
JLFJ_ACTIVE_LOC = FOREACH JLFJ_ACTIVE_LOC GENERATE category_id, location_id, start_date, adid;
JLFJ_ACTIVE_LOC = FILTER JLFJ_ACTIVE_LOC BY (start_date is not null);

JLO = ORDER JLFJ_ACTIVE_LOC BY category_id, location_id, start_date desc;
-- STORE JLO INTO '/user/hadoop/data/exp/ad_events/order_by_start_date';
STORE JLO INTO '$outputdir/order_by_start_date';


JLO_DUMP = LIMITDUMP(JLO);
-- DUMP JLO_DUMP;

JLFJ_NO_LOC = FILTER JLJF BY (location_id is null);

JNLF_LIMIT = LIMITDUMP(JLFJ_NO_LOC);
-- DUMP JNLF_LIMIT;

--
-- Dump this to get the count of records found with
-- no corresponding l2 locations
-- If there are too many, we may have a location tree change.
JNLF = ALLCOUNT(JLFJ_NO_LOC);
-- DUMP JNLF;

--
-- Active ads with no start date - that's bad
-- Store that and analyze
--
JLFJ_ACTIVE_NO_START_DATE = FILTER JLFJ_ACTIVE_LOC BY (start_date is not null);
STORE JLFJ_ACTIVE_NO_START_DATE INTO '$outputdir/active_ads_no_start_date';

-- JLJF_NEW = FOREACH JLJ GENERATE JL::adid as adid, JL::active_state as active_state, JL::start_date as start_date, JL::category_id as category_id, (ZALOC::l3_loc == ZALOC::l4_loc?ZALOC::l2_loc:ZALOC::l3_loc) as location_id, JL::location_id as orig_location_id;
