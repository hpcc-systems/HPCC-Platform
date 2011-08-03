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

import sq;
sq.DeclareCommon();

searchPeople := dataset([
            { 'Gavin', 'Hawthorn'},
            {'Elizabeth', 'Windsor'},
            {'Fred','Flintstone'}], { string forename, string surname });


recordof(sqSimplePersonBookIndex) t(searchPeople l, sqSimplePersonBookDs r) := TRANSFORM
    SELF := l;
    SELF := r;
    END;

joinedPeople := JOIN(searchPeople, sqSimplePersonBookDs, left.forename = right.forename and left.surname = right.surname, TRANSFORM(RIGHT), KEYED(sqSimplePersonBookIndex));

output(joinedPeople, { forename, surname, count(books) });

joinedPeople2 := JOIN(searchPeople, sqSimplePersonBookDs, left.forename = right.forename and left.surname = right.surname and (integer1)left.surname != right.aage*99999, TRANSFORM(RIGHT), KEYED(sqSimplePersonBookIndex));

output(joinedPeople2, { forename, surname, count(books) });

output(sqSimplePersonBookIndex, { forename, surname, count(books) });
