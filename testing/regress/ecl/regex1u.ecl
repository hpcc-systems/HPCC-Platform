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
unicode20        surname;
unicode10        forename;
unicode10        userdate;
            END;

namesTable := dataset([
                {u'Halliday',u'Gavin',u'10/14/1998'},
                {u'Halliday',u'Liz',u'12/01/1998'},
                {u'Halliday',u'Jason',u'01/01/2000'},
                {u'MacPherson',u'Jimmy',u'03/14/2003'}
                ]
                ,namesRecord);

searchpattern := u'^(.*)/(.*)/(.*)$';
search := u'10/14/1998';
filtered := namesTable(REGEXFIND(u'^(Mc|Mac)', surname));

output(filtered);

output(namesTable, {(unicode30)REGEXFIND(NOFOLD(searchpattern), userdate, 0), u'!',
                    (unicode30)REGEXFIND(NOFOLD(searchpattern), userdate, 1), u'!',
                    (unicode30)REGEXFIND(NOFOLD(searchpattern), userdate, 2), u'!',
                    (unicode30)REGEXFIND(NOFOLD(searchpattern), userdate, 3), u'\n'});

REGEXFIND(NOFOLD(searchpattern), search, 0);
REGEXFIND(NOFOLD(searchpattern), search, 1);
REGEXFIND(NOFOLD(searchpattern), search, 2);
REGEXFIND(NOFOLD(searchpattern), search, 3);

REGEXFIND(NOFOLD(u'Gavin'),u'Gavin Halliday');

REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin Halliday') AND
REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday') AND
REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin Halliday',NOCASE);
REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday') AND REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday',1) <> 'x';
REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday',NOCASE) AND REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday',1,NOCASE) <> 'x';
REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday') AND REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday',1,NOCASE) <> 'x';
REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday',NOCASE) AND REGEXFIND(NOFOLD(u'GAVIN'),u'Gavin x Halliday',1) <> 'x';
