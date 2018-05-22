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

//Test serialization of various record/dataset types, including default values
IMPORT Std;

s := service
   string dumpRecordType(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='dumpRecordType',fold;
   string dumpRecordTypeNF(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='dumpRecordType';
   data serializeRecordType(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='serializeRecordType',fold;
   data serializeRecordTypeNF(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='serializeRecordType';
end;

rr := record
  set of string set1 { default(['1','4']) };
  unsigned i1 { default(55) };
  string s6 { default('Fred') };
  string48 s48 { default('Fred') };
  big_endian unsigned bu { default(0x1234) };
  packed unsigned pu { default(0x1234) };
  data d6 { default(d'010203') };
  varstring vs { default('010203') };
  varstring48 vs48 { default('010203') };
  qstring qs { default('q010203') };
  qstring8 qs8 { default('q010203') };
  decimal10_4 dec { default(123.4567) };
  unicode u1 { default(u'€Euros') };
  varunicode vu { default(u'€Euros') };
  unicode8 u8 { default(u'€Euros') };
  varunicode8 vu8 { default(u'€Euros') };
  utf8 utf { default(u'€Euros') };
end;

r := record
  string5 s5 { xpath('@s5') };
  string5 s5a;
  embedded dataset(rr) grandchild;
  rr rrf;
  rr;
end;

d := dataset([],{ string stringField; string20 field2, string20 field3, embedded dataset(r) child });

dtype := Std.Type.VRec(RECORDOF(d));

f := dtype.serializeJson();     // folded
nf := s.dumpRecordTypeNF(d[1]);  // not folded

f;
nf;
f = nf;

fd := dtype.serialize();     // folded
nfd := s.serializeRecordTypeNF(d[1]);  // not folded

fd;
nfd;
fd = nfd;

nestedRecord := RECORD
    boolean hasForename;
    unsigned surnameCount;
    IFBLOCK (SELF.hasForename)
        STRING forename;
    END;
    IFBLOCK (SELF.surnameCount != 0)
        STRING surname;
    END;
END;

mainRecord := RECORD
    UNSIGNED id;
    nestedRecord first;
    nestedRecord second;
    DATASET(nestedRecord) nestedRecord;
//    DATASET(nestedRecord, COUNT(SELF.numNested)) nestedRecord;
    IFBLOCK(SELF.id in [1,3,5,7,12])
         STRING extra;
    END;
END;

dx := dataset([], mainRecord);

fx := s.dumpRecordType(dx[1]);     // folded
nfx := s.dumpRecordTypeNF(dx[1]);  // not folded

fx;
nfx;
fx = nfx; // currently false because Folded generating of meta for ifblocks is not yet implemented.

fdx := s.serializeRecordType(dx[1]);     // folded
nfdx := s.serializeRecordTypeNF(dx[1]);  // not folded

fdx;
nfdx;
fdx = nfdx;
