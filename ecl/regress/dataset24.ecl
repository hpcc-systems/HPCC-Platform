/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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
string10        surname;
string10        forename;
integer2        age := 25;
            END;

unsigned zero := 0 : stored('zero');
namesTable2 := dataset([
        {'Hawthorn','Gavin',31+zero},
        {'Hawthorn','Peter',30+zero},
        {'Smithe','Simon',10+zero},
        {'X','Z',zero}], namesRecord);

output(namesTable2,,'out.d00',overwrite);
