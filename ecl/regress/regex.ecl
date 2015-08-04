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
