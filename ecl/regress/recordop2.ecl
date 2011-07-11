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

childRecord := RECORD
string10            location;
boolean             found;
                end;
                                
baseRecord := RECORD
unsigned8       id;
string20        surname;
            end;

namesRecord := 
            RECORD(baseRecord)
string10        forename;
integer2        age := 25;
            END;

extraRecord := RECORD
unsigned6       id;
integer2        age;
string13        phone;
string          email;
childRecord     x;
            END;

//Clashing types
output(dataset('ds1', namesRecord AND extraRecord, thor),,'o1');
output(dataset('ds2', namesRecord OR extraRecord, thor),,'o2');
output(dataset('ds3', namesRecord AND NOT extraRecord, thor),,'o3');
output(dataset('ds4', namesRecord - extraRecord, thor),,'o4');

//Non existant field
output(dataset('ds5', namesRecord - x, thor),,'o5');
output(dataset('ds6', namesRecord - [id, x], thor),,'o6');

//No resulting fields
output(dataset('ds7', namesRecord and not namesrecord, thor),,'o7');

