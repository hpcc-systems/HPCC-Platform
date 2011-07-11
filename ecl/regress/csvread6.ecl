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


rawFile := dataset([
            {'on'},
            {' on'},
            {' on '},
            {'On'},
            {' oN'},
            {'true'},
            {' true'},
            {' true '},
            {'TRue'},
            {' trUE'},
            {'1'},
            {'2'},
            {'9'},
            {'0001'},
            {'0009'},
            {'  100'},
            {' 0009 '},

            {'o n'},
            {'tru e'},
            {'0000'},
            {''}], { string line });

output(rawFile,,'~csvBool',csv,overwrite);

inFile := dataset('~csvBool', { boolean isOk }, csv);
output(inFile);
