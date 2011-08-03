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


nameRecord :=
            RECORD
string20        surname;
string10        forename;
            END;


leftRecord := RECORD
    unsigned4   uid;
    nameRecord  name;
    unsigned    age;
END;

rightRecord := RECORD
    unsigned4   uid;
    nameRecord  name;
    string      address;
    udecimal8   dob;
END;

leftDs := DATASET('people', leftRecord, thor);
rightDs := DATASET('addresses', rightRecord, thor);

output(join(leftDs, rightDs, LEFT.uid = RIGHT.uid, full only));
