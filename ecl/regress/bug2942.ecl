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

#option ('globalFold', false);
#option ('optimizeGraph', false);

r := record
   string9 ssn;
end;

stTrue := true : stored('stTrue');

em := dataset('email',r,flat);
emx := dataset('email',r,flat);

new_em := if(stTrue, em, emx);

rec := record
    string9 ssnx := new_em.ssn;
end;

rec_em := table(new_em, rec);

output(rec_em);

//This code does, and ...

new_em2 := em;

rec2 := record
    string9 ssn := em.ssn;
end;

rec_em2 := table(new_em2, rec);

//This code does.

new_em3 := if(stTrue, em, em);

output(new_em3);
