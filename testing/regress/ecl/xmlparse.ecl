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

lineRec := { string line; };

//You should really be able to define this inline....
passportRec := 
                RECORD
string              id;
string              idType;
string              issuer;
string              country;
integer             age;
                END;

rec :=      RECORD
string          id;
unicode         fullname;
unicode         title;
passportRec     passport;
string          line;
            END;


rec t(lineRec l) := 
        TRANSFORM
            self.id := xmltext('@eid');
            self.fullname := xmlunicode('ATTRIBUTE[@name="fullname"]');
            self.title := xmlunicode('ATTRIBUTE[@name="honorific"]');
            self.passport.id := xmltext('ATTRIBUTEGROUP[@descriptor="passport"]/ATTRIBUTE[@name="idNumber"]');
            self.passport.idType := xmltext('ATTRIBUTEGROUP[@descriptor="passport"]/ATTRIBUTE[@name="idType"]');
            self.passport.issuer := xmltext('ATTRIBUTEGROUP[@descriptor="passport"]/ATTRIBUTE[@name="issuingAuthority"]');
            self.passport.country := xmltext('ATTRIBUTEGROUP[@descriptor="passport"]/ATTRIBUTE[@name="country"]/@value');
            self.passport.age := (integer)xmltext('ATTRIBUTEGROUP[@descriptor="passport"]/ATTRIBUTE[@name="age"]/@value');
            SELF := l;
        END;


in1 := dataset([{
'<ENTITY eid="P101" type="PERSON" subtype="MILITARY">' +
'<ATTRIBUTE name="fullname">JOHN SMITH</ATTRIBUTE>' +
'<ATTRIBUTE name="honorific">Mr.</ATTRIBUTE>' +
'<ATTRIBUTEGROUP descriptor="passport">' +
'  <ATTRIBUTE name="idNumber">W12468</ATTRIBUTE>' +
'  <ATTRIBUTE name="idType">pp</ATTRIBUTE>' +
'  <ATTRIBUTE name="issuingAuthority">JAPAN PASSPORT' +
'AUTHORITY</ATTRIBUTE>' +
'  <ATTRIBUTE name="country" value="L202"/>' +
'</ATTRIBUTEGROUP>' +
'</ENTITY>'}], lineRec);

test := parse(in1, line, t(LEFT), XML('/ENTITY[@type="PERSON"]'));
output(test);


in2 := dataset([
        {'<QLRecord qlrid="afasdf"><Name><Forename>Gavin</Forename><surname>Halliday</surname></Name><age>33</age></QLRecord>'},
        {'<QLRecord qlrid="afavsdf"><Name><Forename>David</Forename><surname>Bayliss</surname></Name><age>35</age></QLRecord>'}], lineRec);

dataRecord :=   RECORD
string              id := xmltext('@qlrid');
unicode             forename := xmlunicode('Name/Forename');
string              surname := xmltext('Name/surname');
unsigned1           age := (integer)xmltext('age');
string              line := in2.line;
                END;

test2 := parse(in2, line, dataRecord, XML('/QLRecord'));
output(test2);

dataRecord2 :=  RECORD
string              text := xmltext('<>');
                END;

test3 := parse(in2, line, dataRecord2, XML('/QLRecord'));
output(test3);
