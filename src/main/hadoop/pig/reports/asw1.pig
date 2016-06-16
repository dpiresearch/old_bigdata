--
-- This gets all search words results calculated by the previous process
-- and adds human readble categories and locations to the results
--
-- Invoked by runSearchReport.sh
--

-- REGISTER '$deploydir/lib/kernel-pig.jar'
REGISTER '$deploydir/lib/piggybank.jar'

A = LOAD '$inputdir/$alldate/*/search_results.log*' using PigStorage('\u0001') as (version, time, site, macid, clientid, catid, locid, searchstr:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray, listing_count:int);

F = FOREACH A GENERATE time, country, macid, catid, locid, clientid, SUBSTRING(searchstr,0,INDEXOF(searchstr,'|',0)) as kw, listing_count;

FF = FILTER F by (kw != '' AND kw != 'EMPTY' AND kw != 'UNKNOWN');
FF = FILTER FF by (macid != '' AND macid != 'UNKNOWN');

G = GROUP FF BY (country, catid, locid, kw);
GF = FOREACH G GENERATE FLATTEN(group), COUNT(FF), (int)MIN(FF.listing_count) as listing_total;

ASWF = FOREACH GF GENERATE $0 as country, $1 as catid, $2 as locid, $3 as kw, $4 as kwcount, listing_total as listing_total;

ASWFL = LIMIT ASWF 10;
-- DUMP ASWFL;

--
-- Load the location id-name 
--
LOCREF_ZA = LOAD '$refdir/geotz_za.out' USING PigStorage('|') as (id:long,name:chararray);

ALL_LOC = FOREACH LOCREF_ZA GENERATE id as id, name as name;

--
-- Load the category id and names
--
CATREF = LOAD '$refdir/categories.csv' USING PigStorage('|') as (sid:long, id0:long, id1:long,id2:long, id3:long, id4:long, id5:long, name:chararray);
ALL_CAT = FOREACH CATREF GENERATE id5 as id, name as name;

-- 
-- JOIN TO GET HUMAN READABLE CATEGORIES AND LOCATIONS
--

SVJ_J = JOIN ALL_CAT by id RIGHT OUTER, ASWF by catid;
SVJ_JF = FOREACH SVJ_J GENERATE ASWF::country as country, ASWF::catid as catid, ALL_CAT::name as name, ASWF::locid as locid, ASWF::kw as kw, ASWF::kwcount as kwcount;

SVJ_L = JOIN ALL_LOC by id RIGHT OUTER, SVJ_JF by locid;
SVJ_LF = FOREACH SVJ_L GENERATE SVJ_JF::country as country, SVJ_JF::catid as catid, SVJ_JF::name as catname, SVJ_JF::locid as locid, ALL_LOC::name as locname, SVJ_JF::kw as kw, SVJ_JF::kwcount as kwcount;

-- TODO Remove this limit
SVJFFL = LIMIT SVJ_LF 10;
-- DUMP SVJFFL;

STORE SVJ_LF into '$outputdir/$daydate/asw';

