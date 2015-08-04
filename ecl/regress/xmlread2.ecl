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

/*

<QLDATA>
<ENTITY eid="P101" type="PERSON" subtype="MILITARY">
<ATTRIBUTE name="fullname">JOHN SMITH</ATTRIBUTE>
<ATTRIBUTE name="honorific">Mr.</ATTRIBUTE>
<ATTRIBUTEGROUP descriptor="passport">
  <ATTRIBUTE name="idNumber">W12468</ATTRIBUTE>
  <ATTRIBUTE name="idType">pp</ATTRIBUTE>
  <ATTRIBUTE name="issuingAuthority">JAPAN PASSPORT
AUTHORITY</ATTRIBUTE>
  <ATTRIBUTE name="country" value="L202"/>
</ATTRIBUTEGROUP>
</ENTITY>
<ENTITY eid="L202" type="LOCATION" subtype="COUNTRY">
  <ATTRIBUTE name="name">JAPAN</ATTRIBUTE>
</ENTITY>
<RELATIONSHIP eid="R1001" type="LOCATION" subtype="PERSON_BIRTHPLACE">
<ENTITY1 id="P101"/>
<ENTITY2 id="L202"/>
</RELATIONSHIP>
</QLDATA>

*/

//You should really be able to define this inline....
passportRec :=
                RECORD
string              id{xpath('ATTRIBUTE[@name="idNumber"]'),xmldefault('?')};
string              idType{xpath('ATTRIBUTE[@name="idType"]')};
string              issuer{xpath('ATTRIBUTE[@name="issuingAuthority"]')};
string              country{xpath('ATTRIBUTE[@name="country"]/@value')};
integer             age{xpath('ATTRIBUTE[@name="age"]/@value'),xmldefault('99')};
                END;

rec :=      RECORD
string          id{xpath('@eid'),xmldefault(U'-Ä£-')};
unicode         fullname{xpath('ATTRIBUTE[@name="fullname"]'),xmldefault(U'Gavin Ä £100')};
unicode         title{xpath('ATTRIBUTE[@name="honorific"]')};
passportRec     passport{xpath('ATTRIBUTEGROUP[@descriptor="passport"]/')};
            END;


test := dataset('~in.xml', rec, XML('/QLDATA/ENTITY[@type="PERSON"]'));
output(test,,'out.d00',XML);

/*

<?xml version="1.0" encoding="UTF-8">
<QLDataset qldsid="dfkj">
<QLRecord qlrid="afasdf"><Name><Forename>Gavin</Forename><surname>Hawthorn</surname></Name><age>33</age></QLRecord>
<QLRecord qlrid="afavsdf"><Name><Forename>David</Forename><surname>Bayliss</surname></Name><age>35</age></QLRecord>
</QLDataset>

*/



nameRecord :=
                RECORD
string              forename{xpath('Forename')};
string              surname;
                END;

dataRecord :=   RECORD
string              id{xpath('@qlrid')};
nameRecord          fullname{xpath('Name/')};
unsigned1           age;
                END;


test2 := dataset('~in2.xml', datarecord, XML('/QLDataset/QLRecord'));
output(test2,,'out2.d00',XML);
