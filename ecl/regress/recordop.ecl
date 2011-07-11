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
unsigned8       id;
integer2        age;
string13        phone;
string          email;
childRecord     x;
            END;

output(dataset('ds1', namesRecord AND extraRecord, thor),,'o1');
output(dataset('ds2', namesRecord OR extraRecord, thor),,'o2');
output(dataset('ds3', namesRecord AND NOT extraRecord, thor),,'o3');
output(dataset('ds4', namesRecord - extraRecord, thor),,'o4');
output(dataset('ds5', namesRecord - id, thor),,'o5');
output(dataset('ds6', namesRecord - [id, surname], thor),,'o6');
output(dataset('ds7', namesRecord and not age, thor),,'o7');
output(dataset('ds8', namesRecord and not [id, surname, forename], thor),,'o8');

//More nasty: ifblocks....

biggerRecord := RECORD(baseRecord)
boolean             hasExtra;
                    ifblock(self.hasExtra)
extraRecord             extra;
                    END;
                END;

output(dataset('ds11', biggerRecord, thor),,'o11');
output(dataset('ds12', biggerRecord - surname, thor),,'o12');
output(dataset('ds13', biggerRecord - hasExtra, thor),,'o13');
output(dataset('ds14', biggerRecord - extra, thor),,'o14');
