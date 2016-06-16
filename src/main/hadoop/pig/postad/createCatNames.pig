--
-- This script extracts the category names (labels) of the leaf id and it's parents
--
-- Output is category leaf id, l1, l2, l3, leaf (l4) location names
--
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

CAT_FILE = LOAD '$refdir/$catfile' USING PigStorage(',');
ZACAT = FOREACH CAT_FILE GENERATE (long)$0 as site, (long)$1 as l1_cat, (long)$2 as l2_cat, (long)$3 as l3_cat, (long)$4 as l4_cat, (chararray)$5 as catname;
ZACAT = FILTER ZACAT BY (site==3);

NAME = FOREACH ZACAT GENERATE l4_cat as l4_cat, catname as catname;

ZJ3 = JOIN ZACAT BY (l3_cat), NAME BY (l4_cat);
ZJ3F = FOREACH ZJ3 GENERATE ZACAT::l1_cat as l1_cat, ZACAT::l2_cat as l2_cat, ZACAT::l3_cat as l3_cat, NAME::catname as l3_catname, ZACAT::l4_cat as l4_cat, ZACAT::catname as l4_catname;

ZL3 = LIMITDUMP(ZJ3F);
-- DUMP ZL3;

ZJ2 = JOIN ZJ3F BY (l2_cat), NAME BY (l4_cat);
ZJ2F = FOREACH ZJ2 GENERATE ZJ3F::l4_cat as l4_cat, ZJ3F::l1_cat as l1_cat, ZJ3F::l2_cat as l2_cat, NAME::catname as l2_catname, ZJ3F::l3_catname as l3_catname, ZJ3F::l4_catname as l4_catname;

ZL2 = LIMITDUMP(ZJ2F);
-- DUMP ZL2;

ZJ1 = JOIN ZJ2F BY (l1_cat), NAME BY (l4_cat);
ZJ1F = FOREACH ZJ1 GENERATE ZJ2F::l4_cat as l4_cat, NAME::catname as l1_catname, ZJ2F::l2_catname as l2_catname, ZJ2F::l3_catname as l3_catname, ZJ2F::l4_catname as l4_catname;

ZL1 = LIMITDUMP(ZJ1F);
-- DUMP ZL1;

STORE ZJ1F into '$outputdir/catnames/us';

