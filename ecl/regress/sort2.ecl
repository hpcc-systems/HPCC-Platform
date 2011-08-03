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


