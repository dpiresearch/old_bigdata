--
--

-- REGISTER '$deploydir/lib/pig_udf_ref.jar'
-- REGISTER '$deploydir/lib/piggybank.jar'

-- SET default_parallel 50;

--
-- LOAD data since the beginning of time - or however much data we have
--


F1 = LOAD '$inputdir/$thedate/*/post_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,macid_noop:chararray, clientid:chararray,username:chararray,macid:chararray,catid:long, locid:long, user_agent:chararray, country:chararray, language:chararray, field_errors:chararray, attr_errors:chararray, other_errors:chararray);

F1G = FOREACH F1 GENERATE country, language, catid, locid, field_errors, attr_errors, other_errors;

-- FILTERS OUT successful posts
F1G_SUCCESS = FILTER F1G BY (field_errors == 'FIELD|NONE' AND attr_errors == 'ATTR|NONE' AND other_errors == 'OTHER|NONE');
-- Filters out failed posts
F1G_ERROR = FILTER F1G BY (field_errors != 'FIELD|NONE' OR attr_errors != 'ATTR|NONE' OR other_errors != 'OTHER|NONE');


-- sanity check
F1GL_SUCCESS = LIMIT F1G_SUCCESS 10;
-- DUMP F1GL_SUCCESS;

F1GL_ERROR = LIMIT F1G_ERROR 10;
-- DUMP F1GL_ERROR;

F1GG = GROUP F1G BY kw;
F1GGF = FOREACH F1GG GENERATE group as kw, COUNT(F1G) as kwcount;
O1 = ORDER F1GGF BY kwcount DESC;
O1L = LIMIT O1 100;
-- DUMP O1L;
-- STORE O1 INTO '/user/hadoop/data/exp/top_words';

--
-- ### Attribute errors
--

-- get Attribute errors only
F1ATTR = FILTER F1G BY (attr_errors != 'ATTR|NONE');
F1ATTR1 = FOREACH F1ATTR GENERATE country, language, catid, locid, SUBSTRING(attr_errors,INDEXOF(attr_errors,'|',0) + 1,(int) SIZE(attr_errors)) as errorvals;

F1ATTR2 = FOREACH F1ATTR1 GENERATE country, language, catid, locid, TOKENIZE(errorvals,'|') as errorbag;
F1ATTR3 = FOREACH F1ATTR2 GENERATE country, language, catid, locid, flatten(errorbag);

F1ATTR3 = FOREACH F1ATTR3 GENERATE country, language, catid, locid, $4 as error;
-- sanity check
F1ATTRL = LIMIT F1ATTR3 10;
DESCRIBE F1ATTRL;
DUMP F1ATTRL;
-- DUMP F1ATTR3;

G3 = GROUP F1ATTR3 BY (country, catid, locid, error);
G3G = FOREACH G3 GENERATE flatten(group), COUNT(F1ATTR3);
-- DUMP G3G;
STORE G3G INTO '$outputdir/$thedate/attr_errors_by_ccle';

--
-- ### Field errors
--
-- get field errors only
F1FLD = FILTER F1G BY (field_errors != 'FIELD|NONE');
F1FLD1 = FOREACH F1FLD GENERATE country, language, catid, locid, SUBSTRING(field_errors,INDEXOF(field_errors,'|',0) + 1, (int) SIZE(field_errors)) as errorvals;

F1FLD2 = FOREACH F1FLD1 GENERATE country, language, catid, locid, TOKENIZE(errorvals,'|') as errorbag;
F1FLD3 = FOREACH F1FLD2 GENERATE country, language, catid, locid, flatten(errorbag);

F1FLD3 = FOREACH F1FLD3 GENERATE country, language, catid, locid, $4 as error;

-- sanity check
F1FLDL = LIMIT F1FLD3 10;
DESCRIBE F1FLDL;
-- DUMP F1FLD3;

FLDG = GROUP F1FLD3 BY (country, language, catid, locid, error);
FLDGF = FOREACH FLDG GENERATE flatten(group), COUNT(F1FLD3);
STORE FLDGF INTO '$outputdir/$thedate/field_errors_by_ccle';  

--
-- ### Other errors
--
-- get other errors only
F1OTH = FILTER F1G BY (other_errors != 'OTHER|NONE');
F1OTH1 = FOREACH F1OTH GENERATE country, language, catid, locid, SUBSTRING(other_errors,INDEXOF(other_errors,'|',0) + 1,(int) SIZE(other_errors)) as errorvals;

F1OTH2 = FOREACH F1OTH1 GENERATE country, language, catid, locid, TOKENIZE(errorvals,'|') as errorbag;
F1OTH3 = FOREACH F1OTH2 GENERATE country, language, catid, locid, flatten(errorbag);

F1OTH3 = FOREACH F1OTH3 GENERATE country, language, catid, locid, $4 as error;

-- sanity check
F1OTHL = LIMIT F1OTH3 10;
DESCRIBE F1OTHL;
-- DUMP F1OTHL;

OTHG = GROUP F1OTH3 BY (country, language, catid, locid, error);
OTHGF = FOREACH OTHG GENERATE flatten(group), COUNT(F1OTH3);
STORE OTHGF INTO '$outputdir/$thedate/other_errors_by_ccle';  

