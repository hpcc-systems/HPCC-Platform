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

#option ('globalFold', false)

namesRecord := 
            RECORD
string20        surname;
string10        forename;
string10        userdate;
            END;

namesTable := dataset([
                {'Halliday','Gavin','10/14/1998'},
                {'Halliday','Liz','12/01/1998'},
                {'Halliday','Jason','01/01/2000'},
                {'MacPherson','Jimmy','03/14/2003'}
                ]
                ,namesRecord);

searchpattern := '^(.*)/(.*)/(.*)$';
search := '10/14/1998';
filtered := namesTable(REGEXFIND('^(Mc|Mac)', surname));

output(filtered);

output(namesTable, {(string30)REGEXFIND(NOFOLD(searchpattern), userdate, 0), '!',
                    (string30)REGEXFIND(NOFOLD(searchpattern), userdate, 1), '!',
                    (string30)REGEXFIND(NOFOLD(searchpattern), userdate, 2), '!',
                    (string30)REGEXFIND(NOFOLD(searchpattern), userdate, 3), '\n'});

REGEXFIND(NOFOLD(searchpattern), search, 0);
REGEXFIND(NOFOLD(searchpattern), search, 1);
REGEXFIND(NOFOLD(searchpattern), search, 2);
REGEXFIND(NOFOLD(searchpattern), search, 3);

REGEXFIND(NOFOLD('Gavin'),'Gavin Halliday');

REGEXFIND(NOFOLD('GAVIN'),'Gavin Halliday') AND
REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday') AND
REGEXFIND(NOFOLD('GAVIN'),'Gavin Halliday',NOCASE);
REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday') AND REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday',1) <> 'x';
REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday',NOCASE) AND REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday',1,NOCASE) <> 'x';
REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday') AND REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday',1,NOCASE) <> 'x';
REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday',NOCASE) AND REGEXFIND(NOFOLD('GAVIN'),'Gavin x Halliday',1) <> 'x';
