/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

// Test various record translations

// All try to translate to one of these two structures

// for simple cases

dest := RECORD
  string20 desc       { default('Not matched')};
  boolean b           { default(true)};
  unsigned4 n0        { default(0)};
  unsigned4 n1        { default(1)};
  unsigned8 n2        { default(2)};
  real r1             { default(3.14)};
  decimal4_2 d1       { default(-3.14)};
  udecimal4_2 d2      { default(3.14)};
  set of string2 set1 { default(['s1','s2']) };
  string8 s1          { default('string01') };
  varstring v1        { default('v1') };
  varstring5 v2       { default('v2') };
  unicode8 u1         { default(u'€unicode') };
  varunicode vu1      { default(u'€vu1') };
  varunicode5 vu2     { default(u'€vu2') };
  data da1            { default(d'11223344') };
  data8 da2           { default(d'55667788') };
  string tail         { default('end') };
END;

// for recursive tests

child := RECORD
  string4 sub1 { default('sub1')};
  string4 sub2 { default('sub2')};
END;

parent := RECORD
  string20 desc;
  linkcounted dataset(child) subl;
  embedded dataset(child) sube;
  child subrec;
END;

s := SERVICE
   streamed dataset(dest) stransform(streamed dataset input) : eclrtl,pure,library='eclrtl',entrypoint='transformRecord',passParameterMeta(true);
   streamed dataset(parent) rtransform(streamed dataset input) : eclrtl,pure,library='eclrtl',entrypoint='transformRecord',passParameterMeta(true);
END;

// Untranslated (type descriptors match)

untranslated := dataset([{'Untranslated'}], dest);

// None needed (only defaults differ)

dest_nn := RECORD
  string20 desc       { default('default') };
  boolean b           { default(true)};
  unsigned4 n0        { default(0)};
  unsigned4 n1        { default(1)};
  unsigned8 n2        { default(2)};
  real r1             { default(3.14)};
  decimal4_2 d1       { default(-3.14)};
  udecimal4_2 d2      { default(3.14)};
  set of string2 set1 { default(['s1','s2']) };
  string8 s1          { default('string01') };
  varstring v1        { default('v1') };
  varstring5 v2       { default('v2') };
  unicode8 u1         { default(u'€unicode') };
  varunicode vu1      { default(u'€vu1') };
  varunicode5 vu2     { default(u'€vu2') };
  data da1            { default(d'11223344') };
  data8 da2           { default(d'55667788') };
  string tail         { default('end') };
END;

none_needed := dataset([{'None needed'}], dest_nn);

// Fields removed at end

dest_removed := RECORD
  string20 desc       { default('Not matched')};
  boolean b           { default(true)};
  unsigned4 n0        { default(0)};
  unsigned4 n1        { default(1)};
  unsigned8 n2        { default(2)};
  real r1             { default(3.14)};
  decimal4_2 d1       { default(-3.14)};
  udecimal4_2 d2      { default(3.14)};
  set of string2 set1 { default(['s1','s2']) };
  string8 s1          { default('string01') };
  varstring v1        { default('v1') };
  varstring5 v2       { default('v2') };
  unicode8 u1         { default(u'€unicode') };
  varunicode vu1      { default(u'€vu1') };
  varunicode5 vu2     { default(u'€vu2') };
  data da1            { default(d'11223344') };
  data8 da2           { default(d'55667788') };
  string tail         { default('end') };
  string gone         { default('gone') };
END;

removed := dataset([{'Removed'}], dest_removed);

// Fields swapped (only defaults differ)

dest_swapped := RECORD
  string20 desc       { default('default') };
  boolean b           { default(true)};
  unsigned4 n1        { default(1)};
  unsigned4 n0        { default(0)};
  unsigned8 n2        { default(2)};
  real r1             { default(3.14)};
  decimal4_2 d1       { default(-3.14)};
  udecimal4_2 d2      { default(3.14)};
  set of string2 set1 { default(['s1','s2']) };
  string8 s1          { default('string01') };
  varstring v1        { default('v1') };
  varstring5 v2       { default('v2') };
  unicode8 u1         { default(u'€unicode') };
  varunicode vu1      { default(u'€vu1') };
  varunicode5 vu2     { default(u'€vu2') };
  data da1            { default(d'11223344') };
  data8 da2           { default(d'55667788') };
  string tail         { default('end') };
END;

swapped_only := dataset([{'Swapped'}], dest_swapped);

// Translating embedded to link counted and vice versa

parent1 := RECORD
  string20 desc;
  embedded dataset(child) subl;
  linkcounted dataset(child) sube;
END;

switch_linkcounts := dataset([{'Switch linkcounts',[{'e1','e1a'},{'e2'}],[{'e3'},{'e4'}]}], parent1);

// Copying linkcounted datasets via link

parent2 := RECORD
  string20 desc;
  linkcounted dataset(child) subl;
END;

translate_link := dataset([{'Link child',[{'l1','l1a'},{'l2'}]}], parent2);

// Translating fields in nested datasets/records

child1 := record
  string5 sub1 { default('ts1')};
END;

parent3 := record
  STRING20 desc;
  LINKCOUNTED dataset(child1) subl;
  EMBEDDED dataset(child1) sube;
  child1 subrec;
END;

translate_nested := dataset([{'Translate nested', [{'t1  a'},{'t2  a'}],[{'t3  a'},{'t4  a'}],{'t5  a'}}], parent3);

// Translating a set from one type to another

dest_set := record
  STRING20 desc;
  set of string3 set1;
END;

translate_set := dataset([{'Translate set', ['aab','aac']},{'Translate all', ALL},{'Translate empty set', []}], dest_set);

// Translate where no records match

dest_nomatch := record
  STRING20 notdesc;
END;

nomatch := dataset([{'No match'}], dest_nomatch);

// Test multiple adjacent matching fields

dest_multi_adjacent := RECORD
  string20 desc;
  unsigned4 n1        { default(1)};
  unsigned8 n2        { default(2)};

  varstring v1        { default('v1') };
  varstring5 v2       { default('v2') };
END;

multi_adjacent := dataset([{'Multiple adjacent'}], dest_multi_adjacent);

// Test truncating strings

dest_truncate := RECORD
  string20 desc;
  string18 s1          { default('STRING012345') };
END;

truncate_strings := dataset([{'Truncate strings'}], dest_truncate);

// Test extending strings

dest_extend := RECORD
  string20 desc;
  string7 s1           { default('STRING1') };
END;

extend_strings := dataset([{'Extend strings'}], dest_extend);

// Test typecasts

declare_typecast_from_int(t, name, pname) := MACRO
#uniquename(dest_typecast_from_int)
#uniquename(transform_typecast_from_int)
%dest_typecast_from_int% := RECORD
  string20 desc;

  t b     { default(1)};
  t n1    { default(3)};
  t n0    { default(2)};  // Note swapped
  t n2    { default(4)};
  t r1    { default(5)};
  t d1    { default(6)};
  t d2    { default(7)};
  set of t set1 { default([8,9]) };
  t s1    { default(10) };
  t v1    { default(11) };
  t v2    { default(12) };
  t u1    { default(13) };
  t vu1   { default(14) };
  t vu2   { default(15) };
  t da1   { default(16) };
  t da2   { default(17) };
END;

dest %transform_typecast_from_int%(%dest_typecast_from_int% L) := transform
  self.desc := L.desc;
  self.b := (boolean) L.b;
  self.n0 := (unsigned4) L.n0;
  self.n1 := (unsigned4) L.n1;
  self.n2 := (unsigned8) L.n2;
  self.r1 := (real) L.r1;
  self.d1 := (decimal4_2) L.d1;
  self.d2 := (udecimal4_2) L.d2;
  self.set1 := (set of string2) L.set1;
  self.s1 := (string8) L.s1;
  self.v1 := (varstring) L.v1;
  self.v2 := (varstring5) L.v2;
  self.u1 := (unicode8) L.u1;
  self.vu1 := (varunicode) L.vu1;
  self.vu2 := (varunicode5) L.vu2;
  self.da1 := (data) L.da1;
  self.da2 := (data8) L.da2;
  self := [];
END;

name := dataset([{'Typecast ' + #TEXT(t)}], %dest_typecast_from_int%);
pname := PROJECT(nofold(name), %transform_typecast_from_int%(LEFT));
ENDMACRO;

declare_typecast_from_string(t, name, pname) := MACRO
#uniquename(dest_typecast_from_string)
#uniquename(transform_typecast_from_string)
%dest_typecast_from_string% := RECORD
  string20 desc;

  t b    { default('1')};
  t n1    { default('3')};
  t n0    { default('2')};  // Note swapped
  t n2    { default('4')};
  t r1    { default('5')};
  t d1    { default('6')};
  t d2    { default('7')};
  set of t set1 { default(['8','9']) };
  t s1    { default('10') };
  t v1    { default('11') };
  t v2    { default('12') };
  t u1    { default('13') };
  t vu1   { default('14') };
  t vu2   { default('15') };
  t da1   { default('16') };
  t da2   { default('17') };
END;

dest %transform_typecast_from_string%(%dest_typecast_from_string% L) := transform
  self.desc := L.desc;
  self.b := (boolean) L.b;
  self.n0 := (unsigned4) L.n0;
  self.n1 := (unsigned4) L.n1;
  self.n2 := (unsigned8) L.n2;
  self.r1 := (real) L.r1;
  self.d1 := (decimal4_2) L.d1;
  self.d2 := (udecimal4_2) L.d2;
  self.set1 := (set of string2) L.set1;
  self.s1 := (string8) L.s1;
  self.v1 := (varstring) L.v1;
  self.v2 := (varstring5) L.v2;
  self.u1 := (unicode8) L.u1;
  self.vu1 := (varunicode) L.vu1;
  self.vu2 := (varunicode5) L.vu2;
  self.da1 := (data) L.da1;
  self.da2 := (data8) L.da2;
  self := [];
END;


name := dataset([{'Typecast ' + #TEXT(t)}], %dest_typecast_from_string%);
pname := PROJECT(nofold(name), %transform_typecast_from_string%(LEFT));
ENDMACRO;

declare_typecast_from_int(integer4, typecast_from_int4, project_from_int4);
declare_typecast_from_int(integer2, typecast_from_int2, project_from_int2);
declare_typecast_from_int(real4, typecast_from_real4, project_from_real4);
declare_typecast_from_int(real8, typecast_from_real8, project_from_real8);
declare_typecast_from_int(packed integer, typecast_from_packed, project_from_packed);
declare_typecast_from_int(packed unsigned, typecast_from_upacked, project_from_upacked);
declare_typecast_from_int(big_endian unsigned, typecast_from_bigendian, project_from_bigendian);
declare_typecast_from_int(decimal4_2, typecast_from_decimal, project_from_decimal);
declare_typecast_from_int(unsigned decimal4_2, typecast_from_udecimal, project_from_udecimal);

declare_typecast_from_string(string, typecast_from_string, project_from_string);
declare_typecast_from_string(varstring, typecast_from_varstring, project_from_varstring);
declare_typecast_from_string(string8, typecast_from_string8, project_from_string8);
declare_typecast_from_string(varstring8, typecast_from_varstring8, project_from_varstring8);
declare_typecast_from_string(unicode, typecast_from_unicode, project_from_unicode);
declare_typecast_from_string(varunicode, typecast_from_varunicode, project_from_varunicode);
declare_typecast_from_string(unicode8, typecast_from_unicode8, project_from_unicode8);
declare_typecast_from_string(varunicode8, typecast_from_varunicode8, project_from_varunicode8);
declare_typecast_from_string(utf8, typecast_from_utf8, project_from_utf8);

sequential(
  OUTPUT(s.stransform(untranslated)),
  OUTPUT(s.stransform(none_needed)),
  OUTPUT(s.stransform(removed)),
  OUTPUT(s.stransform(swapped_only)),
  OUTPUT(s.rtransform(switch_linkcounts)),
  OUTPUT(s.rtransform(translate_link)),
  OUTPUT(s.rtransform(translate_nested)),
  OUTPUT(s.stransform(translate_set)),
  OUTPUT(s.stransform(nomatch)),
  OUTPUT(s.stransform(multi_adjacent)),
  OUTPUT(s.stransform(truncate_strings)),
  OUTPUT(s.stransform(extend_strings)),
  OUTPUT(s.stransform(typecast_from_int4)),
  OUTPUT(project_from_int4),
  OUTPUT(s.stransform(typecast_from_int2)),
  OUTPUT(project_from_int2),
  OUTPUT(s.stransform(typecast_from_real4)),
  OUTPUT(project_from_real4),
  OUTPUT(s.stransform(typecast_from_real8)),
  OUTPUT(project_from_real8),
  OUTPUT(s.stransform(typecast_from_packed)),
//  OUTPUT(project_from_packed),  // internal compiler error

  OUTPUT(s.stransform(typecast_from_upacked)),
//  OUTPUT(project_from_upacked),
  OUTPUT(s.stransform(typecast_from_bigendian)),
  OUTPUT(project_from_bigendian),
  OUTPUT(s.stransform(typecast_from_decimal)),
  OUTPUT(project_from_decimal),
  OUTPUT(s.stransform(typecast_from_udecimal)),
  OUTPUT(project_from_udecimal),
  OUTPUT(s.stransform(typecast_from_string)),
  OUTPUT(project_from_string),
  OUTPUT(s.stransform(typecast_from_varstring)),
  OUTPUT(project_from_varstring),
  OUTPUT(s.stransform(typecast_from_string8)),
  OUTPUT(project_from_string8),
  OUTPUT(s.stransform(typecast_from_varstring8)),
  OUTPUT(project_from_varstring8),
  OUTPUT(s.stransform(typecast_from_unicode)),
  OUTPUT(project_from_unicode),
  OUTPUT(s.stransform(typecast_from_varunicode)),
  OUTPUT(project_from_varunicode),
  OUTPUT(s.stransform(typecast_from_unicode8)),
  OUTPUT(project_from_unicode8),
  OUTPUT(s.stransform(typecast_from_varunicode8)),
  OUTPUT(project_from_varunicode8),
  OUTPUT(s.stransform(typecast_from_utf8)),
  OUTPUT(project_from_utf8),
);
