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
string10            forename;
string10            surname;
string2             nl := '\r\n';
        END;

names2Record :=
        RECORD
string10            forename;
string10            surname;
        END;

OUTPUT(PIPE(DATASET('csv1',namesRecord,thor),'pipeWrite csv2csv1', names2Record, CSV, OUTPUT(CSV)),,'csvo1');
OUTPUT(PIPE(DATASET('csv2',namesRecord,thor),'pipeWrite csv2csv2', names2Record, CSV(QUOTE([])), OUTPUT(CSV)),,'csvo2');
OUTPUT(PIPE(DATASET('csv3',namesRecord,thor),'pipeWrite xml2csv1', names2Record, CSV, OUTPUT(XML)),,'csvo3');
OUTPUT(PIPE(DATASET('csv4',namesRecord,thor),'pipeWrite xml2csv2', names2Record, CSV(QUOTE([]),TERMINATOR([])), OUTPUT(XML(Heading('<a>','</a>'))), REPEAT),,'csvo4');
OUTPUT(PIPE(DATASET('csv5',namesRecord,thor),'pipeWrite xml2csv3', names2Record, CSV, OUTPUT(XML), GROUP),,'csvo5');
OUTPUT(PIPE(GROUP(DATASET('csv8',namesRecord,thor),ROW),'pipeWrite xml2csv6', names2Record, CSV, OUTPUT(XML)),,'csvo8');

OUTPUT(PIPE(DATASET('xml1',namesRecord,thor),'pipeWrite csv2xml1', names2Record, XML, OUTPUT(CSV)),,'xmlo1');
OUTPUT(PIPE(DATASET('xml2',namesRecord,thor),'pipeWrite csv2xml2', names2Record, XML('abc/def/ghi'), OUTPUT(CSV)),,'xmlo2');
OUTPUT(PIPE(DATASET('xml3',namesRecord,thor),'pipeWrite xml2xml1', names2Record, XML, OUTPUT(XML)),,'xmlo1');
OUTPUT(PIPE(DATASET('xml4',namesRecord,thor),'pipeWrite csv2xml2', names2Record, XML('abc/def/ghi'), OUTPUT(CSV)),,'xmlo2');
