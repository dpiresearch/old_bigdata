--
-- This script extracts the location names (labels) of the leaf id and it's parents
--
-- Output is location leaf id, l1 loc name, l2 loc name, l3 locl name, leaf (l4) loc name
--
-- Depends on a geotz_za.out file which maps location nodes to their corresponding labels

--
-- Usage:
--     pig -param refdir=/user/hadoop/data/pp/ref -param locfile=za_loc.csv -f createLocNames.pig
--     
--

DEFINE ALLCOUNT(A) RETURNS G1F {
  G1 = GROUP $A ALL;
  $G1F = FOREACH G1 GENERATE COUNT($A);
}

DEFINE LIMITDUMP(INLIMIT) RETURNS OUTLIMIT {
  $OUTLIMIT = LIMIT $INLIMIT 50;
  -- $OUTLIMIT = TMPL;
}

LOC_FILE = LOAD '$refdir/{$country}_loc.csv' USING PigStorage(' ');
ZACAT = FOREACH LOC_FILE GENERATE (long)$0 as l1_cat, (long)$1 as l2_cat, (long)$2 as l3_cat, (long)$3 as l4_cat;
-- ZACAT = FOREACH LOC_FILE GENERATE (long)$0 as site, (long)$1 as l1_cat, (long)$2 as l2_cat, (long)$3 as l3_cat, (long)$4 as l4_cat;
-- ZACAT = FILTER ZACAT BY (site==1000);

LOC1 = LOAD '$refdir/geotz_{$country}.out' USING PigStorage('|');
NAME = FOREACH LOC1 GENERATE (long) $0 as l4_cat, (chararray) $1 as catname;

NL = LIMITDUMP(NAME);
ZL = LIMITDUMP(ZACAT);

-- DUMP NL;
-- DUMP ZL;
ZJ4 = JOIN ZACAT BY (l4_cat), NAME BY (l4_cat);
ZJ4F = FOREACH ZJ4 GENERATE ZACAT::l1_cat as l1_cat, ZACAT::l2_cat as l2_cat, ZACAT::l3_cat as l3_cat, ZACAT::l4_cat as l4_cat, NAME::catname as l4_catname;

ZJ3 = JOIN ZJ4F BY (l3_cat), NAME BY (l4_cat);
ZJ3F = FOREACH ZJ3 GENERATE ZJ4F::l1_cat as l1_cat, ZJ4F::l2_cat as l2_cat, ZJ4F::l3_cat as l3_cat, NAME::catname as l3_catname, ZJ4F::l4_cat as l4_cat, ZJ4F::l4_catname as l4_catname;

ZL3 = LIMITDUMP(ZJ3F);
-- DUMP ZL3;

ZJ2 = JOIN ZJ3F BY (l2_cat), NAME BY (l4_cat);
ZJ2F = FOREACH ZJ2 GENERATE ZJ3F::l4_cat as l4_cat, ZJ3F::l1_cat as l1_cat, ZJ3F::l2_cat as l2_cat, NAME::catname as l2_catname, ZJ3F::l3_catname as l3_catname, ZJ3F::l4_catname as l4_catname;

ZL2 = LIMITDUMP(ZJ2F);
-- DUMP ZL2;

ZJ1 = JOIN ZJ2F BY (l1_cat), NAME BY (l4_cat);
ZJ1F = FOREACH ZJ1 GENERATE ZJ2F::l4_cat as l4_cat, (NAME::catname=='root'?'All US':NAME::catname) as l1_catname, ZJ2F::l2_catname as l2_catname, ZJ2F::l3_catname as l3_catname, ZJ2F::l4_catname as l4_catname;

ZL1 = LIMITDUMP(ZJ1F);
DUMP ZL1;

STORE ZJ1F into '/user/hadoop/data/pp/ref/locnames/$country';
-- STORE ZJ1F into '/user/hadoop/exp/locnames/za';

