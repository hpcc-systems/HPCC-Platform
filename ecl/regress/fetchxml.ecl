/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
