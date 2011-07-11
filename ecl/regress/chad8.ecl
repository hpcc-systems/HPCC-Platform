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

rec := record
   unsigned4 pad1;
   unsigned1 score_field;
   unsigned8 pad2;
   unsigned6 did_field;
   string63  pad3;
   string10  phone_field;
   end;

infile := dataset('x', rec, thor);

__didfilter__ := infile.did_field = 0 or infile.score_field <> 100;
__go__ := length(trim((string)(integer)infile.phone_field))=10 and (__didfilter__);

__804__ := __go__;



y := infile(~__804__);
output(y);



