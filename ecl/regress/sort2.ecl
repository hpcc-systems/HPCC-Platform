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

/*--SOAP--
<message name="Specialized Vehicle Search">
    <part name="plate" type="xsd:string"/>
    <part name="zip" type="tns:EspStringArray"/>
    <part1 name="zip" type="tns:string"/>
</message>
*/
/*--INFO-- This is the Specialized Vehicle Search service. Under development.
*/
/*--HELP--
Use at your own risk
*/

export fdle_keys_test := macro

namesRecord :=
RECORD
    string5  zip5;
    string20        surname := '?????????????';
    string10        forename := '?????????????';
    integer2        age := 25;
END;

addressRecord :=
RECORD
    string5  zip5;
    string30        addr := 'Unknown';
    string20        surname;
END;

namesTable := dataset([
        {'12345','Smithe','Pru',10},
        {'12345','Hawthorn','Gavin',31},
        {'12345','Hawthorn','Mia',30},
        {'12345','Smith','Jo'},
        {'12345','Smith','Matthew'},
        {'12345','X','Z'}], namesRecord);

addressTable := dataset([
        {'12345','Hawthorn','10 Slapdash Lane'},
        {'12345','Smith','Leicester'},
        {'12345','Smith','China'},
        {'12345','X','12 The burrows'},
        {'12345','X','14 The crescent'},
        {'12345','Z','The end of the world'}
        ], addressRecord);

dNamesTable := namesTable;//distribute(namesTable, hash(surname));
dAddressTable := addressTable;//distributed(addressTable, hash(surname));

JoinRecord :=
RECORD
    string5         zip5;
    string20        surname;
    string10        forename;
    integer2        age := 25;
    string30        addr;
END;

JoinRecord JoinTransform (namesRecord l, addressRecord r) :=
TRANSFORM
    SELF.addr := r.addr;
    SELF := l;
END;


JoinedF := join (dNamesTable, dAddressTable,
                LEFT.surname[1..10] = RIGHT.surname[1..10] AND
                LEFT.surname[11..16] = RIGHT.surname[11..16] AND
                LEFT.forename[1] <> RIGHT.addr[1],
                JoinTransform (LEFT, RIGHT), keep(1), LEFT OUTER);


varstring _PLATE_NUMBER := '' : stored('plate');
set of string5 _ZIP5 := [] : stored('zip');

filter :=
        (_ZIP5 = [] or JoinedF.ZIP5 in _ZIP5)
        //(_ZIP5 = '' or indexmatched.ZIP5 = _ZIP5)
        ;

output(JoinedF(filter))


endmacro;

fdle_keys_test()


