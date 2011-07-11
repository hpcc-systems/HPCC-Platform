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

(string)(decimal1_1) 5.6;
(string)(decimal2_2) 5.06;
output((string)(DECIMAL4_1)0);              // = 0.0
output((string)(DECIMAL4_1)0.0);            // = 0.0
output((string)(DECIMAL5_2)999.999);        // = 0.0  loss of precision


(decimal1_1) 5.6;
(decimal2_2) 5.06;
output((DECIMAL4_1)0);              // = 0.0
output((DECIMAL4_1)0.0);            // = 0.0
output((DECIMAL5_2)999.999);        // = 0.0  loss of precision
