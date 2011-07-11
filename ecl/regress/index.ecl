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

person := dataset('person', { unsigned8 person_id, unsigned per_xval, string40 per_first_name, string40 per_last_name, data9 per_cid, unsigned8 xpos }, thor);

myNames := [ person.per_last_name+'x', person.per_last_name+'a', person.per_last_name+'z', person.per_last_name+'v'];
myValues := [person.per_xval+10,person.per_xval, person.per_xval+20, person.per_xval+5 ];

output(person,{myNames[RANKED(1,myNames)],myValues[RANKED(1,myValues)]});


