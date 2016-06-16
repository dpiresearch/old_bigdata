--
--    This pig script aggregates all phone clicks for a give ad.
--
--    Input file: client_events.log
--    Output: (adid, userid, count)
--
--

SET default_parallel 4;

EVTS = LOAD '$inputdir/*/*/client_events.log*' USING PigStorage('\u0001') as (version:int, time:long, eventype:int, eventdata:chararray, macid:chararray, ip:chararray, host:chararray, ua:chararray,referer:chararray);

-- Filer only PhoneClick events
EVT_CLK = FILTER EVTS BY eventype == 1;

CLK = FOREACH EVT_CLK GENERATE (LONG) REGEX_EXTRACT(eventdata,'([\\d]*)\\|([\\d]*)',1) as adid, (LONG) REGEX_EXTRACT(eventdata,'([\\d]*)\\|([\\d]*)',2) as usrid, 1 as count;

CLK_GRP = GROUP CLK BY (adid, usrid);

SUM_CLKS = FOREACH CLK_GRP GENERATE FLATTEN(group), SUM(CLK.count) as count;

-- Load baseline Phone Clicks (From Global Platform)
CLK_GP = LOAD '$oldClicks' USING PigStorage(',') as (adid:long, usrid:long, count:long);

-- Join Sum Clicks with Global Clicks
CLK_J = JOIN SUM_CLKS BY (adid, usrid) LEFT OUTER, CLK_GP BY (adid, usrid);

CLK_J_T = FOREACH CLK_J GENERATE SUM_CLKS::group::adid, SUM_CLKS::group::usrid, SUM_CLKS::count + (CLK_GP::count is null ? 0 : CLK_GP::count);

STORE CLK_J_T INTO '$outputdir/$thedate';

