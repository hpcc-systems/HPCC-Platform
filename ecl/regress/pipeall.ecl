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
string2             nl := '\r\n';
        END;

cmd := 'pipeWrite \\thordata\\names.new' : stored('cmd');

d := PIPE('pipeRead 200', namesRecord);
t := PIPE(d, 'pipeThrough');
output(t,,PIPE('pipeWrite \\thordata\\names.all'));
output(t,{surname,forename},PIPE(cmd));
output(t,,PIPE('pipeWrite \\thordata\\names.all '+t.surname,repeat));
output(t,,PIPE('pipeWriteNext ' ,repeat));
//output(t,{surname},PIPE('pipeWrite \\thordata\\names.all '+t.surname,repeat));        // currently unsupported

output(PIPE(t,'pipeWrite \\thordata\\names.all '+t.surname,repeat));

