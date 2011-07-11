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

Layout_l := {
REAL x;
REAL y;
};

ds := DATASET([
{1.0, 1.0}
, {0.0, 2.0}
, {1.0, 3.0}
, {0.0, 4.0}
, {1.0, 5.0}
, {0.0, 6.0}
, {1.0, 7.0}
, {0.0, 8.0}
, {1.0, 9.0}
, {0.0, 10.0}
, {1.0, 11.0}
, {0.0, 12.0}
, {1.0, 13.0}
], Layout_l);

varxy := COVARIANCE(ds, x, y);
output(varxy);

