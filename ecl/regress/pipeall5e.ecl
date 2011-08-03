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

namesRecord :=
        RECORD
string10            forename;
string10            surname;
set of string       invalidin;
string2             nl := '\r\n';
        END;

names2Record :=
        RECORD
string10            forename;
string10            surname;
set of string       invalidout;
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
