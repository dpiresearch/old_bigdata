--
--    This pig script dedups counts (vip/phoneclicks) between two dates.
--
--    Input file: 
-- 			a) /{thedate}/part*   (adid, usrid, count)
--			b) /{thedate-1}/part* (adid, usrid, count)
--    Output: (adid, usrid, count)
--
--

EVT_NOW = LOAD '$inputdir/$thedate/part*' as (adid:long, usrid:long, count:long);
EVT_BEFORE = LOAD '$inputdir/$olddate/part*' as (adid:long, usrid:long, count:long);

-- Join current and old data, retain any new entries from current. 
J_L = JOIN EVT_NOW BY (adid, usrid) LEFT OUTER, EVT_BEFORE BY (adid, usrid);

-- Filter updated counts & new counts
J_L_FLT = FILTER J_L BY (EVT_NOW::count > EVT_BEFORE::count) OR (EVT_BEFORE::count is null);

-- Trim to three columns
RES = FOREACH J_L_FLT GENERATE EVT_NOW::adid, EVT_NOW::usrid, EVT_NOW::count;

STORE RES INTO '$outputdir/$thedate';
