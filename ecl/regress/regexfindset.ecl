/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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


/* ascii strings */
str := 'add-subtract 123.90, or 123';
REGEXFINDSET(NOFOLD('(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)'), str);

REGEXFINDSET('(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', str);

/* Unicode strings */
ustr := U'add-subtract 123.90, or 123';
REGEXFINDSET(NOFOLD(U'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)'), ustr);

REGEXFINDSET(U'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', ustr);

/* in Project test */
RecLayout := RECORD
    STRING50 rawstr;
END;
ParsedLayout := RECORD
    SET OF STRING parsed;
END;

ParsedLayout parseThem(recLayout L) := TRANSFORM
    SELF.parsed := REGEXFINDSET('(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', l.rawstr);
END;

recTable := dataset([
    {'subtract 35 from 50'},
    {'divide 100 by 25 and add 5'}
    ], RecLayout);

PROJECT(recTable, parseThem(LEFT));

recTable2 := NOFOLD(dataset([
    {'subtract 35 from 50'},
    {'divide 100 by 25 and add 5'}
    ], RecLayout));

PROJECT(recTable2, parseThem(LEFT));

URecLayout := RECORD
    UNICODE50 rawstr;
END;
UParsedLayout := RECORD
    SET OF UNICODE parsed;
END;

UParsedLayout uparseThem(URecLayout L) := TRANSFORM
    SELF.parsed := REGEXFINDSET(U'(([[:alpha:]]|(-))+)|([[:digit:]]+([.][[:digit:]]+)?)', l.rawstr);
END;

urecTable := dataset([
    {U'subtract 35 from 50'},
    {U'divide 100 by 25 and add 5'}
    ], URecLayout);

PROJECT(urecTable, uparseThem(LEFT));

urecTable2 := NOFOLD(dataset([
    {U'subtract 35 from 50'},
    {U'divide 100 by 25 and add 5'}
    ], URecLayout));

PROJECT(urecTable2, uparseThem(LEFT));
