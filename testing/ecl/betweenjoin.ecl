/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

boolean sliding := true;
#if (sliding)
#option ('slidingJoins', true);
#end

namesRecord := 
            RECORD
string20        surname;
integer2        age;
integer2        dadAge;
integer2        mumAge;
            END;

PairNamesRecord := RECORD
namesRecord L;
namesRecord R;
                   END;

PairNamesRecord t(NamesRecord L, NamesRecord r) := TRANSFORM
  SELF.r := r;
  SELF.l := l;
END;

namesTable := dataset([
  {'SMITH',30,40,40},
  {'JONES',40,50,51}
  ],namesRecord);
namesTable2 := dataset([
  {'SMATH',20,40,40},
  {'SMATH',22,40,40},
  {'SMETH',30,50,51},
  {'JONES',40,40,40},
  {'JUNES',50,40,40},
  {'WHITE',60,40,40}
],namesRecord);

integer2 aveAge(namesRecord r) := (r.dadAge+r.mumAge)/2;

// Standard join on a function of left and right
output(join(namesTable, namesTable2, aveAge(left) = aveAge(right),t(LEFT,RIGHT) #if (sliding=false) ,ALL #end ));

//Several simple examples of sliding join syntax
output(join(namesTable, namesTable2, left.age >= right.age - 10 and left.age <= right.age +10,t(LEFT,RIGHT) #if (sliding=false) ,ALL #end ));
output(join(namesTable, namesTable2, left.age between right.age - 10 and right.age +10,t(LEFT,RIGHT) #if (sliding=false) ,ALL #end ));
output(join(namesTable, namesTable2, left.age between right.age + 10 and right.age +30,t(LEFT,RIGHT) #if (sliding=false) ,ALL #end ));
output(join(namesTable, namesTable2, left.age between (right.age + 20) - 10 and (right.age +20) + 10,t(LEFT,RIGHT) #if (sliding=false) ,ALL #end));
output(join(namesTable, namesTable2, aveAge(left) between aveAge(right)+10 and aveAge(right)+40,t(LEFT,RIGHT) #if (sliding=false) ,ALL #end));

//Same, but on strings.  Also includes age to ensure sort is done by non-sliding before sliding.
output(join(namesTable, namesTable2, left.surname between right.surname[1..2]+'AAAAAAAAAA' and right.surname[1..10]+'ZZZZZZZZZZ' and left.age=right.age,t(LEFT,RIGHT) #if (sliding=false) ,ALL #end));
