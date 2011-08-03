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

subRec := record
big_endian unsigned4 id;
packed integer2 score;
        end;

rec := record
unsigned6    did;
subRec       primary;
string       unkeyable;
subRec       secondary;
        end;


unsigned4 searchId := 0 : stored('searchId');
unsigned4 secondaryScore := 0 : stored('secondaryScore');

rawfile := dataset('~thor::rawfile', rec, THOR, preload);

// Combine matches
filtered := rawfile(
 keyed(secondary.id = searchId),
 keyed(primary.score = secondaryScore)
);

output(filtered)
