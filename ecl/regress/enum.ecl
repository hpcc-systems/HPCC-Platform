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


//Check the simple varieties
GenderEnum := enum(unsigned1, Male,Female,Either,Unknown);
PersonFlags := enum(None = 0, Dead=1, Foreign=2,Terrorist=4,Wanted=Terrorist*2);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
GenderEnum      gender;
integer2        age := 25;
            END;

namesTable2 := dataset([
        {'Hawthorn','Gavin',GenderEnum.Male,PersonFlags.Foreign},
        {'Bin Laden','Osama',GenderEnum.Male,PersonFlags.Foreign+PersonFlags.Terrorist+PersonFlags.Wanted}
        ], namesRecord);

output(namesTable2);



// Nasty - an enum defined inside a module - the values for the enum need to be bound later.

myModule(unsigned4 baseError, string x) := MODULE

export ErrorCode := ENUM(ErrorBase = baseError,
                  ErrNoActiveTable,
                  ErrNoActiveSystem,
                  ErrFatal,
                  ErrLast);


export reportX := FAIL(ErrorCode.ErrNoActiveTable, 'No ActiveTable in '+x);
end;

myModule(100, 'Call1').reportX;
myModule(300, 'Call2').reportX;


//Another nasty - typedef an enum from a child module - hit problems with not binding correctly
ErrorCodes := myModule(999, 'Call3').errorCode;
output(ErrorCodes.ErrFatal);
output(myModule(1999, 'Call4').errorCode.ErrFatal);
