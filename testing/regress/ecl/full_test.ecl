/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//TEST: thor=1 hthor=1 roxie=0
App_1(file,outname) := macro
#uniquename(rec)
%rec% := record
  file;
  unsigned1 one := 1;
  end;

outname := table(file,%rec%);

  endmacro;

Check_b10(file,stri) := macro
#uniquename(b)
%b% := 2;
'Should be 10 records ('+stri+'):'+(string)count(file);
'Should be 0 records ('+stri+'):'+(string)count(file(cnt<>100000));
  endmacro;

fname_i := dataset([{'DAVID     '},
                  {'CLARE     '},
                  {'KELLY     '},
                  {'KIMBERLY  '},
                  {'PAMELA    '},
                  {'JEFFREY   '},
                  {'MATTHEW   '},
                  {'LUKE      '},
                  {'JOHN      '},
                  {'EDWARD    '}
                  ],{ string10 name });

Lname_i := dataset([{'BAYLISS   '},
                  {'DOLSON    '},
                  {'BILLINGTON'},
                  {'SMITH     '},
                  {'JONES     '},
                  {'ARMSTRONG '},
                  {'LINDHORFF '},
                  {'SIMMONS   '},
                  {'WYMAN     '},
                  {'MIDDLETON '}
                  ],{ string10 name });

PRANGE_i := dataset([{1},
                   {2},
                   {3},
                   {4},
                   {5},
                   {6},
                   {7},
                   {8},
                   {9},
                   {10}
                   ],{ unsigned8 number });

street_i:= dataset([{'HIGH      '},
                  {'MILL      '},
                  {'CITATION  '},
                  {'25TH      '},
                  {'ELGIN     '},
                  {'VICARAGE  '},
                  {'VICARYOUNG'},
                  {'PEPPERCORN'},
                  {'SILVER    '},
                  {'KENSINGTON'}
                  ],{ string10 name });

ZIPS_i   := dataset([{1},
                   {2},
                   {3},
                   {4},
                   {5},
                   {6},
                   {7},
                   {8},
                   {9},
                   {10}
                   ],{ unsigned8 number });

AGE_i    := dataset([{31},
                   {32},
                   {33},
                   {34},
                   {35},
                   {36},
                   {37},
                   {38},
                   {39},
                   {40}
                   ],{ unsigned2 number });

app_1(fname_i,o_fname)
app_1(lname_i,o_lname)
app_1(prange_i,o_prange)
app_1(street_i,o_street)
app_1(zips_i,o_zips)
app_1(age_i,o_age)

Full_Format := record
  string10 fname := o_fname.name;
  string10 lname := '';
  unsigned2 prange := 0;
  string10 street := '';
  unsigned3 zips := 0;
  unsigned1 age := 0;
  unsigned1 one := 1;
  unsigned8 id := 0;
  end;

base_fn := table(o_fname,full_format);

base_fn_1 := distribute(base_fn,1);

full_format add_l(base_fn le, o_lname ri) := transform
  self.lname := ri.name;
  self := le;
  end;

base_fln := join(base_fn,o_lname,left.one=right.one,add_l(left,right));

base_fln_1 := join(base_fn_1,distribute(o_lname,1),left.one=right.one,add_l(left,right),local);

full_format add_p(base_fn le, o_prange ri) := transform
  self.prange := ri.number;
  self := le;
  end;

base_flpn := join(base_fln,o_prange,left.one=right.one,add_p(left,right));

base_flpn_1 := join(base_fln_1,distribute(o_prange,1),left.one=right.one,add_p(left,right),local);

full_format add_s(base_fn le, o_street ri) := transform
  self.street := ri.name;
  self := le;
  end;

base_flpsn := join(base_flpn,o_street,left.one=right.one,add_s(left,right));

base_flpsn_1 := join(base_flpn_1,distribute(o_street,1),left.one=right.one,add_s(left,right),local);

full_format add_z(base_fn le, o_zips ri) := transform
  self.zips := ri.number;
  self := le;
  end;

base_flpszn := join(base_flpsn,o_zips,left.one=right.one,add_z(left,right));
base_flpszn_1 := join(base_flpsn_1,distribute(o_zips,1),left.one=right.one,add_z(left,right),local);

full_format add_a(base_fn le, o_age ri) := transform
  self.age := ri.number;
  self := le;
  end;

base_flpszan := join(base_flpszn,o_age,left.one=right.one,add_a(left,right));
base_flpszan_1 := join(base_flpszn_1,distribute(o_age,1),left.one=right.one,add_a(left,right),local);

/*count(base_fn);
count(base_fln);
count(base_flpn);
count(base_flpsn);
count(base_flpszn);*/
'Should equal a million :'+(string)count(base_flpszan);
'Should equal a million (local):'+(string)count(base_flpszan_1);

res := dedup(base_flpszan+base_flpszan_1,fname,lname,prange,street,zips,age,all);

'Should equal a million (joined):' + (string)count(res);

r := record
  res.fname;
  unsigned4 cnt := count(GROUP);
  end;

tfew := table(res,r,res.fname,few);
tfull := table(res,r,res.fname);
tfl := table(distribute(res,hash(fname)),r,res.fname,local);

check_b10(tfew,'table-Few')
check_b10(tfull,'table-Full')
check_b10(tfl,'table-local')

fcount := record
  res.fname;
  unsigned4 cnt := 1;
  end;

fres := table(res,fcount);

sfres := sort(fres,fname);

dfres := sort(distribute(fres,hash(fname)),fname,local);

gsfres := group(sfres,fname);

gdfres := group(dfres,fname,local);

fcount incr(fcount le, fcount ri) := transform
  self.cnt := le.cnt + ri.cnt;
  self := le;
  end;

rfull := rollup(sfres,fname,incr(left,right));
rgroup := rollup(gsfres,1,incr(left,right));
rlocal := rollup(dfres,fname,incr(left,right),local);
rlgrp := rollup(gdfres,1,incr(left,right));

check_b10(rfull,'Rollup Full')
check_b10(rgroup,'Rollup Grouped')
check_b10(rlocal,'Rollup Local')
check_b10(rlgrp,'Rollup Local Grouped')

fcount cnt(fcount le, fcount ri) := transform
  self.cnt := IF(le.fname=ri.fname,le.cnt+ri.cnt,ri.cnt);
  self := ri;
  end;

ifull := dedup(sort(iterate(sfres,cnt(left,right)),fname,-cnt),fname);
igroup := dedup(sort(iterate(gsfres,cnt(left,right)),fname,-cnt),fname);
ilocal := dedup(sort(iterate(dfres,cnt(left,right),local),fname,-cnt,local),fname,local);
ilgrp := dedup(sort(iterate(gdfres,cnt(left,right)),fname,-cnt),fname);

check_b10(ifull,'Iterate Full')
check_b10(igroup,'Iterate Grouped')
check_b10(ilocal,'Iterate Local')
check_b10(ilgrp,'Iterate Local Grouped')
