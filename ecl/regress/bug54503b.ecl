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