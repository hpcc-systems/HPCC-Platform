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
set of string       invalid;
string2             nl := '\r\n';
        END;

output(PIPE('pipeRead csv1', namesRecord, CSV),,'csv1');
output(PIPE('pipeRead csv2', namesRecord, CSV(QUOTE([]))),,'csv2');

output(PIPE('pipeRead xml1', namesRecord, XML),,'xml1');
output(PIPE('pipeRead xml2', namesRecord, XML('abc/def/ghi')),,'xml2');
