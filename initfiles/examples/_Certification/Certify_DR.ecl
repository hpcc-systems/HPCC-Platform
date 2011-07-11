/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Certification;

check_b10(file,stri,num) := MACRO
  #uniquename(b)
  %b% := 2;
  stri + ' (Should be '+ num + ' records): '+(STRING) COUNT(file);
ENDMACRO;

total_nodes := Certification.Setup.NodeMult1 * Certification.Setup.NodeMult2;

//base_flpszanbm := Certification.DataFile;
base_flpszanbm := DISTRIBUTE(Certification.DataFile,RANDOM());
base_flpszanbm_1 := DISTRIBUTE(base_flpszanbm,HASH32(fname, lname, prange, street, zips, age, birth_state, birth_month));

tbl_flpszanbm := TABLE(base_flpszanbm,
                       {base_flpszanbm.fname,
                        base_flpszanbm.lname,
                        base_flpszanbm.prange,
                        base_flpszanbm.street,
                        base_flpszanbm.zips,
                        base_flpszanbm.age,
                        base_flpszanbm.birth_state,
                        base_flpszanbm.birth_month,
                        base_flpszanbm.one,
                        base_flpszanbm.id});
tbl_flpszanbm_1 := DISTRIBUTE(tbl_flpszanbm,HASH32(fname,lname, prange,street,zips,age,birth_state,birth_month));

// Full global join
full_global_join := JOIN(base_flpszanbm,base_flpszanbm_1, LEFT.id=RIGHT.id);
'Full Global Join - should = '+ total_nodes + ' million : '+(STRING) COUNT(full_global_join);

// local join
local_join := JOIN(base_flpszanbm_1,base_flpszanbm_1, LEFT.id=RIGHT.id, LOCAL);
'Local Join - should = '+ total_nodes + ' million (local): '+(STRING) COUNT(local_join);

// dedup to verify there is only 1x2x3x4x5x6x7x8 records
res := DEDUP(base_flpszanbm+base_flpszanbm_1, fname, lname, prange, street, zips, age, birth_state, birth_month, ALL);
'Dedup - should = '+ total_nodes + ' million (joined): ' + (STRING) COUNT(res);

// to be used for testing compressed I/O
res1 := dedup(tbl_flpszanbm+tbl_flpszanbm_1, fname, lname, prange, street, zips, age, birth_state, birth_month, ALL) : persist('persist::res1');
'Complex I/O - should = '+ total_nodes + ' million: ' + (STRING) COUNT(res1);

r := RECORD
  res.fname;
  UNSIGNED4 cnt := COUNT(GROUP);
END;

//hash aggregate
tfew := TABLE(res, r, res.fname, FEW);
//global aggregate
tfull := table(res, r, res.fname);
//local aggregate
tfl := TABLE(DISTRIBUTE(res, HASH32(fname)), r, res.fname, LOCAL);

check_b10(tfew, 'Hash Aggregate', Certification.Setup.NodeMult1)
check_b10(tfull, 'Global Aggregate', Certification.Setup.NodeMult1)
check_b10(tfl, 'Local Aggregate', Certification.Setup.NodeMult1)

fcount := RECORD
  res.fname;
  res.lname;
  UNSIGNED4 cnt := 1;
END;

fres := TABLE(res, fcount);

//global sort
sfres := SORT(fres, fname, lname);

//local sort, distribute
dfres := SORT(DISTRIBUTE(fres, HASH32(fname)), fname, lname, LOCAL);

//global group
gsfres := GROUP(sfres, fname, lname);

//local group
gdfres := GROUP(dfres, fname, lname, LOCAL);

fcount incr(fcount le, fcount ri) := TRANSFORM
  SELF.cnt := le.cnt + ri.cnt;
  SELF := le;
END;

//global rollup
rfull := ROLLUP(sfres, LEFT.fname=RIGHT.fname, incr(LEFT, RIGHT));
//global grouped rollup
rgroup := ROLLUP(gsfres, 1, incr(LEFT, RIGHT));
//local rollup
rlocal := ROLLUP(dfres, LEFT.fname=RIGHT.fname, incr(LEFT,RIGHT), LOCAL);
//local grouped rollup
rlgrp := ROLLUP(gdfres, 1, incr(LEFT, RIGHT));

check_b10(rgroup, 'Global Grouped Rollup', total_nodes)
check_b10(rlocal, 'Local Rollup', Certification.Setup.NodeMult1)
check_b10(rlgrp, 'Local Grouped Rollup', total_nodes)

fcount cnt(fcount le, fcount ri) := TRANSFORM
  SELF.cnt := IF(le.fname=ri.fname AND le.lname=ri.lname, le.cnt+ri.cnt, ri.cnt);
  SELF := ri;
END;

//global iterate/sort/dedup
ifull := DEDUP(SORT(ITERATE(sfres, cnt(LEFT, RIGHT)), fname, lname, -cnt), fname, lname);
//global grouped iterate/sort/dedup
igroup := DEDUP(SORT(ITERATE(gsfres, cnt(LEFT, RIGHT)), fname, lname, -cnt), fname, lname);
//local iterate/sort/dedup
ilocal := DEDUP(SORT(ITERATE(dfres, cnt(LEFT, RIGHT), LOCAL), fname, lname, -cnt, LOCAL), fname, lname, LOCAL);
//local grouped iterate/sort/dedup
ilgrp := DEDUP(SORT(ITERATE(gdfres, cnt(LEFT, RIGHT)), fname, lname, -cnt), fname, lname);

check_b10(ifull, 'Global It/Srt/Ddp', total_nodes)
check_b10(igroup, 'Global Grouped It/Srt/Ddp', total_nodes)
check_b10(ilocal, 'Local It/Srt/Ddp', total_nodes)
check_b10(ilgrp, 'Local Grouped It/Srt/Ddp', total_nodes)

// string search
'String Search Results: '+(STRING) COUNT(base_flpszanbm(fname = 'DIRK', lname = 'BRYANT', prange = 1));
