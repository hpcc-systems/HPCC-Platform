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

#option ('targetClusterType', 'roxie');

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)};
            END;

d := dataset('x',namesRecord,FLAT);

i1 := index(d, { d } ,'\\seisint\\person.name_first.key1');
nameIndexRecord := recordof(i1);

errorRecord := RECORD
unsigned4           code;
string50            msg;
               END;

fullRecord := RECORD(namesRecord)
errorRecord     err;
            END;

noError := 0 : stored('NoError');
myCode := 999 : stored('myCode');

fullRecord t(nameIndexRecord l, unsigned4 code) := transform
    SELF := l;
    self.err.code := code;
    SELF := [];
END;

fullRecord createError(unsigned4 code, string50 msg) := transform
    SELF.err.code := code;
    SELF.err.msg := msg;
    SELF := [];
END;

//check filter is added to keyed filter onFail transform
//actually folded away completely
res1a := project(i1(surname='Granger'), t(LEFT, 0));
res1b := limit(res1a, 99, ONFAIL(createError(99, 'Too many matching names 5')), keyed);
res1c := res1b(err.code <> 0);
output(res1c);


//check filter is added to keyed filter onFail transform
res3a := project(i1(surname='Granger'), t(LEFT, noError));
res3b := limit(res3a, 99, ONFAIL(createError(99, 'Too many matching names 5')), keyed);
res3c := res3b(err.code <> 0);
output(res3c);

//Ensure the skip isn't constant folded

res2a := project(i1(surname='Wayne'), t(LEFT, 0));
res2b := limit(res2a, 99, ONFAIL(createError(myCode, 'Too many matching names 5')), keyed);
res2c := res2b(err.code = 0);
output(res2c);

/*

//check filter is added to keyed filter onFail transform
//actually folded away completely
res4a := project(i1(surname='Granger'), t(LEFT, 0));
res4b := limit(res4a, 99, ONFAIL(createError(99, 'Too many matching names 5')));
res4c := res4b(err.code <> 0);
output(res4c);


//check filter is added to keyed filter onFail transform
res5a := project(i1(surname='Granger'), t(LEFT, noError));
res5b := limit(res5a, 99, ONFAIL(createError(99, 'Too many matching names 5')));
res5c := res5b(err.code <> 0);
output(res5c);

res6a := project(i1(surname='Wayne'), t(LEFT, 0));
res6b := limit(res6a, 98, ONFAIL(createError(myCode, 'Too many matching names 5')));
res6c := res6b(err.code = 0);
output(res6c);

*/