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

r := record
    unsigned4       seq;
    string20        lname;
    string20        fname;
    unsigned8       pos;
end;

set of unsigned4 u4set := [1,2,3] : stored('u4set');
set of unsigned4 u4set2 := [1,2,3] : stored('u4set2');
set of unsigned8 u8set := [1,2,3] : stored('u8set');
set of string20 s20set := [] : stored('s20set');
set of string40 s40set := [] : stored('s40set');

boolean guard1 := false : stored('guard1');
boolean guard2 := false : stored('guard2');
boolean guard3 := false : stored('guard3');
boolean guard4 := false : stored('guard4');
boolean guard5 := false : stored('guard5');

i := index(r, 'i');

//Simple tests
output(i(keyed(seq = 3)));
output(i(keyed(seq in u4set)));
output(i(keyed(seq in u8set)));

output(i(keyed(seq=1 and lname = 'Hawthorn')));
output(i(keyed(seq=1 and lname in s20set)));
output(i(keyed(seq=1 and lname in s40set)));


//Query invariant AND guard conditions
output(i(keyed(guard1 and seq = 3)));
output(i(keyed(guard1 and seq in u4set)));
output(i(keyed(guard1 and seq=1 and lname in s40set)));

//Query invariant OR guard conditions
output(i(keyed(guard1 or seq = 3)));
output(i(keyed(guard1 or seq in u4set)));
output(i(keyed((guard1 or seq=1) and (guard1 or lname in s40set))));
output(i(keyed(guard1 or seq = 3 or seq=2)));
output(i(keyed(guard1 or (seq=1 and lname in s40set))));                    // more natural expression of

//Both kinds of guard conditions:
output(i(keyed(guard1 and (guard2 or seq = 3))));
output(i(keyed(guard1 and (guard2 or seq in u4set))));
output(i(keyed((guard1 and (guard2 or seq=1) and (guard3 or lname in s40set)))));
output(i(keyed(guard1 and (guard2 or seq = 3 or seq=2))));
output(i(keyed(guard1 and (guard2 or (seq=1 and lname in s40set)))));                   // more natural expression of)

//Both kinds of guard conditions:
output(i(keyed(guard1 or (guard2 and seq = 3))));
output(i(keyed(guard1 or (guard2 and seq in u4set))));
output(i(keyed((guard1 or ((guard2 and seq=1) and (guard2 and lname in s40set))))));
output(i(keyed(guard1 or (guard2 and (seq = 3 or seq=2)))));
output(i(keyed(guard1 or (guard2 and (seq=1 and lname in s40set)))));                   // more natural expression of)


//Complex conditions on a single field
output(i(keyed((guard1 or seq=1) or (guard2 or seq=2))));
output(i(keyed((guard1 and seq=1) or (guard2 and seq=2))));
output(i(keyed((guard1 or seq=1) and (guard2 or seq=2))));
output(i(keyed(seq=1 or (guard2 and (guard3 or seq=2)))));
output(i(keyed(seq=1 or (guard2 or (guard3 and seq=2)))));

output(i(guard1 or seq=1 or seq*seq=2));                // can't key.... so don't include in keyed()

// this is theoretically the only way of triggering the duplicate code, but doesn't actually trigger it at the moment.
// probably worth considering deleteing all referece to duplicate
//output(i(keyed((integer1)seq<>1)));

//More complicated conditions on multiple fields.
output(i(keyed(guard1 or (seq=1 and lname = 'Hawthorn'))));
output(i(keyed((seq=2) and (guard1 or seq=1) and (guard1 or lname = 'Hawthorn'))));     //equivalent to below
output(i(keyed((seq=2) and (guard1 or (seq=1 and lname = 'Hawthorn')))));

//Conditional expressions......
output(i(keyed(IF(guard1,seq = 3,seq=2))));
output(i(keyed(IF(guard1,seq = 3,true))));
output(i(keyed(IF(guard1,seq = 3,guard2))));
output(i(keyed(guard2 or IF(guard1,seq = 3,seq=2))));
output(i(keyed(guard2 or IF(guard1,seq = 3,true))));

//Conditional, with different fields affected.in diff branches.
output(i(wild(seq) and keyed(IF(guard1,seq = 3,lname='Hawthorn'))));
output(i(keyed(IF(guard1,seq = 3,seq=2 and lname='Hawthorn'))));
output(i(keyed(IF(guard1,seq = 3,seq=2 and (guard3 or lname='Hawthorn')))));
output(i(wild(seq) and keyed(guard2 or IF(guard1,seq = 3,lname='Hawthorn'))));
output(i(keyed(guard2 or IF(guard1,seq = 3,seq=2 and lname='Hawthorn'))));
output(i(keyed(guard2 or IF(guard1,seq = 3,seq=2 and (guard3 or lname='Hawthorn')))));

output(i(keyed(guard2 or IF(guard1,seq in u8set,seq=2 and (guard3 or lname in s40set)))));

output(i(keyed(seq in u4set and seq in u8set)));
output(i(keyed(guard1 and (seq in u4set and seq in u8set))));
output(i(keyed(guard1 or (seq in u4set and seq in u8set))));
output(i(keyed(seq = 1 or guard1 or (seq in u4set and seq in u8set))));
output(i(keyed(seq in u4set2 and (guard1 or (seq in u4set and seq in u8set)))));
output(i(keyed((guard1 or seq in u4set) and (guard2 or seq in u8set))));

//NOTE: We can't handle ORs between fields (seq=1) or (lname='x')
