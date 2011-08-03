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

#option ('optimizeIndexSource',true);

import sq;
sq.DeclareCommon();

unsigned zero := 0 : stored('zero');

output(count(sqSimplePersonBookIndex));
output(count(sqSimplePersonBookIndex)=10);
output(count(choosen(sqSimplePersonBookIndex, 20)));
output(count(choosen(sqSimplePersonBookIndex, 20))=10);
output(count(choosen(sqSimplePersonBookIndex, 4)));
output(count(choosen(sqSimplePersonBookIndex, 4))=4);
output(count(choosen(sqSimplePersonBookIndex, zero)));
output(count(choosen(sqSimplePersonBookIndex, zero))=0);

output(count(sqSimplePersonBookIndex(surname != 'Hawthorn')));
output(count(sqSimplePersonBookIndex(surname != 'Hawthorn'))=7);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 20)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 20))=7);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 3)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 3))=3);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), zero)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), zero))=0);
