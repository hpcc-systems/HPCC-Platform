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

nameRecord := RECORD
  STRING name{MAXLENGTH(100)};
END;

akaRecord := RECORD
  BOOLEAN suspicious;
  DATASET(nameRecord) names{MAXCOUNT(10),xpath('names/name')};
END;

personRecord := RECORD
  UNSIGNED1 age;
  akaRecord aka;
END;

phoneRecord := RECORD
  STRING20 phone;
END;

addressRecord := RECORD
  STRING town{MAXLENGTH(100)};
  DATASET(phoneRecord) phones{MAXCOUNT(20),xpath('phones/phone')};
END;


inputFormat := RECORD
  BOOLEAN doQuery;
  personRecord person;
  addressRecord addr;
END;


input := dataset([], inputFormat) : stored('input', FEW);

SEQUENTIAL(
output(input),
output(input[1].person.age)
);

/*
<query>
  <input>
   <row>
    <doquery>1</doquery>
    <person>
      <age>10</age>
      <aka><names><name>William</name><name>Bill</name></names></aka>
    </person>
    <addr>
      <town>London</town>
      <phones><phone>0123456</phone><phones>
    </addr>
   </row>
  </input>
</query>
*/