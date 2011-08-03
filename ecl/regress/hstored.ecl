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

xx := dataset('ff', {string20 per_last_name, unsigned8 holepos}, thor);

ds := xx(per_last_name='Hawthorn');
pagesize := 100 : stored('pagesize');
fpos := 0 : stored('fpos');

result := choosen(ds(holepos >= fpos), pagesize);

//evaluate(result[pagesize], xx.holepos) : stored('lpos');
output(result);

