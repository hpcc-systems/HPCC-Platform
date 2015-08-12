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
