/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
