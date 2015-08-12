/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

//You should really be able to define this inline....
passportRec :=
                RECORD
string              id{xpath('ATTRIBUTE[@name="idNumber"]')};
string              idType{xpath('ATTRIBUTE[@name="idType"]')};
string              issuer{xpath('ATTRIBUTE[@name="issuingAuthority"]')};
string              country{xpath('ATTRIBUTE[@name="country"]/@value')};
integer             age{xpath('ATTRIBUTE[@name="age"]/@value')};
                END;

rec :=      RECORD
string          id{xpath('@eid')};
unicode         fullname{xpath('ATTRIBUTE[@name="fullname"]')};
unicode         title{xpath('ATTRIBUTE[@name="titled"]')};
passportRec     passport{xpath('ATTRIBUTEGROUP[@descriptor="passport"]/')};
            END;


df := dataset('~in.xml', rec, XML('/QLDATA/ENTITY[@type="PERSON"]'));

rs := record
  unsigned1 book := 0;
  unsigned1 chapter := 0;
  unsigned1 verse := 0;
  unsigned8 __fpos{virtual(fileposition)};
end;

Key_KJV_File := INDEX(rs,'~thor::key.kjv.verse');

i := Key_KJV_File(book=1,chapter=1,verse=2);

rec get(df le, i ri) := transform
  self := le;
  end;

fd := fetch(df,i,right.__fpos,get(left, right));

output(fd);
