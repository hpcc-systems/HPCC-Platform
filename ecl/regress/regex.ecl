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

#option ('globalFold', false);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
string10        userdate;
            END;

namesTable := dataset([
                {'Hawthorn','Gavin','10/14/1998'},
                {'Hawthorn','Mia','12/01/1998'},
                {'Hawthorn','Jason','01/01/2000'},
                {'MacPherson','Jimmy','03/14/2003'}
                ]
                ,namesRecord);

searchpattern := '^(.*)/(.*)/(.*)$';
search := '10/14/1998';
filtered := namesTable(REGEXFIND('^(Mc|Mac)', surname));

output(filtered);

output(nofold(namesTable), {(string30)REGEXFIND(searchpattern, userdate, 0), '!',
                    (string30)REGEXFIND(searchpattern, userdate, 1), '!',
                    (string30)REGEXFIND(searchpattern, userdate, 2), '!',
                    (string30)REGEXFIND(searchpattern, userdate, 3), '\n'});

REGEXFIND(searchpattern, search, 0);
REGEXFIND(searchpattern, search, 1);
REGEXFIND(searchpattern, search, 2);
REGEXFIND(searchpattern, search, 3);

regexfind('Gavin','Gavin Hawthorn');

regexfind('GAVIN','Gavin Hawthorn') AND
regexfind('GAVIN','Gavin x Hawthorn') AND
regexfind('GAVIN','Gavin Hawthorn',NOCASE);
regexfind('GAVIN','Gavin x Hawthorn') AND regexfind('GAVIN','Gavin x Hawthorn',1) <> 'x';
regexfind('GAVIN','Gavin x Hawthorn',NOCASE) AND regexfind('GAVIN','Gavin x Hawthorn',1,NOCASE) <> 'x';
regexfind('GAVIN','Gavin x Hawthorn') AND regexfind('GAVIN','Gavin x Hawthorn',1,NOCASE) <> 'x';
regexfind('GAVIN','Gavin x Hawthorn',NOCASE) AND regexfind('GAVIN','Gavin x Hawthorn',1) <> 'x';
