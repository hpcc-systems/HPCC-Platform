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


namesRecord :=
            RECORD
unicode20       surname;
unicode10       forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Hawthorn',U'Gaéëvin',31},
        {'Hawthorn',U'τετελεσταιι',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

output(namesTable2,,'out1.d00',CSV);

output(namesTable2,,'out2.d00',CSV(MAXLENGTH(9999),SEPARATOR(';'),TERMINATOR(['\r\n','\r','\n','\032']),QUOTE(['\'','"'])));
