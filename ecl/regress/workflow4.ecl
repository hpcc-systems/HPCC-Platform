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

#option('workflow', 1);
#option('generateLogicalGraph', true);

layout_hi := RECORD
    STRING20 a;
    UNSIGNED b;
END;

ds := DATASET([{'hi', 0}, {'there', 1}], layout_hi);

layout_hi Copy(ds l) := TRANSFORM
    SELF := l;
END;

norm := NORMALIZE(ds, 2, Copy(LEFT));
norm2 := norm  : SUCCESS(OUTPUT(norm,, 'adtemp::wf_test', OVERWRITE)); 
OUTPUT(COUNT(norm2));

