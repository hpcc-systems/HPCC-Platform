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

#option ('generateLogicalGraph', true);

/**
 * Defines a record that contains information about a person
 */
namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

/**
Defines a table that can be used to read the information from the file
and then do something with it.
 */

namesTable := dataset('x',namesRecord,FLAT);


/**
    Allows the name table to be filtered.
    
    @param  ages    The ages that are allowed to be processed.
            badForename Forname to avoid.

    @return         the filtered dataset.
 */

namesTable filtered2(set of integer2 ages, string badForename) := namesTable(age in ages, forename != badForename);

output(filtered2([10,20,33], ''));
