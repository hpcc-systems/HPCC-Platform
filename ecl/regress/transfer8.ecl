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

did := 0x123456789abcdef;

unsigned1 reverseDidU1(unsigned1 did) := (unsigned1)(transfer(did, big_endian unsigned1));
unsigned1 reverseDidUX1(unsigned1 did) := (unsigned1)(transfer(nofold(did), big_endian unsigned1));
unsigned2 reverseDidU2(unsigned2 did) := (unsigned2)(transfer(did, big_endian unsigned2));
unsigned2 reverseDidUX2(unsigned2 did) := (unsigned2)(transfer(nofold(did), big_endian unsigned2));
unsigned3 reverseDidU3(unsigned3 did) := (unsigned3)(transfer(did, big_endian unsigned3));
unsigned3 reverseDidUX3(unsigned3 did) := (unsigned3)(transfer(nofold(did), big_endian unsigned3));
unsigned4 reverseDidU4(unsigned4 did) := (unsigned4)(transfer(did, big_endian unsigned4));
unsigned4 reverseDidUX4(unsigned4 did) := (unsigned4)(transfer(nofold(did), big_endian unsigned4));
unsigned5 reverseDidU5(unsigned5 did) := (unsigned5)(transfer(did, big_endian unsigned5));
unsigned5 reverseDidUX5(unsigned5 did) := (unsigned5)(transfer(nofold(did), big_endian unsigned5));
unsigned6 reverseDidU6(unsigned6 did) := (unsigned6)(transfer(did, big_endian unsigned6));
unsigned6 reverseDidUX6(unsigned6 did) := (unsigned6)(transfer(nofold(did), big_endian unsigned6));
unsigned7 reverseDidU7(unsigned7 did) := (unsigned7)(transfer(did, big_endian unsigned7));
unsigned7 reverseDidUX7(unsigned7 did) := (unsigned7)(transfer(nofold(did), big_endian unsigned7));
unsigned8 reverseDidU8(unsigned8 did) := (unsigned8)(transfer(did, big_endian unsigned8));
unsigned8 reverseDidUX8(unsigned8 did) := (unsigned8)(transfer(nofold(did), big_endian unsigned8));

output(reverseDidU1(did));
output(reverseDidUX1(did));
output(reverseDidU2(did));
output(reverseDidUX2(did));
output(reverseDidU3(did));
output(reverseDidUX3(did));
output(reverseDidU4(did));
output(reverseDidUX4(did));
output(reverseDidU5(did));
output(reverseDidUX5(did));
output(reverseDidU6(did));
output(reverseDidUX6(did));
output(reverseDidU7(did));
output(reverseDidUX7(did));
output(reverseDidU8(did));
output(reverseDidUX8(did));

integer1 reverseDid1(integer1 did) := (integer1)(transfer(did, big_endian integer1));
integer1 reverseDidX1(integer1 did) := (integer1)(transfer(nofold(did), big_endian integer1));
integer2 reverseDid2(integer2 did) := (integer2)(transfer(did, big_endian integer2));
integer2 reverseDidX2(integer2 did) := (integer2)(transfer(nofold(did), big_endian integer2));
integer3 reverseDid3(integer3 did) := (integer3)(transfer(did, big_endian integer3));
integer3 reverseDidX3(integer3 did) := (integer3)(transfer(nofold(did), big_endian integer3));
integer4 reverseDid4(integer4 did) := (integer4)(transfer(did, big_endian integer4));
integer4 reverseDidX4(integer4 did) := (integer4)(transfer(nofold(did), big_endian integer4));
integer5 reverseDid5(integer5 did) := (integer5)(transfer(did, big_endian integer5));
integer5 reverseDidX5(integer5 did) := (integer5)(transfer(nofold(did), big_endian integer5));
integer6 reverseDid6(integer6 did) := (integer6)(transfer(did, big_endian integer6));
integer6 reverseDidX6(integer6 did) := (integer6)(transfer(nofold(did), big_endian integer6));
integer7 reverseDid7(integer7 did) := (integer7)(transfer(did, big_endian integer7));
integer7 reverseDidX7(integer7 did) := (integer7)(transfer(nofold(did), big_endian integer7));
integer8 reverseDid8(integer8 did) := (integer8)(transfer(did, big_endian integer8));
integer8 reverseDidX8(integer8 did) := (integer8)(transfer(nofold(did), big_endian integer8));

output(reverseDid1(did));
output(reverseDidX1(did));
output(reverseDid2(did));
output(reverseDidX2(did));
output(reverseDid3(did));
output(reverseDidX3(did));
output(reverseDid4(did));
output(reverseDidX4(did));
output(reverseDid5(did));
output(reverseDidX5(did));
output(reverseDid6(did));
output(reverseDidX6(did));
output(reverseDid7(did));
output(reverseDidX7(did));
output(reverseDid8(did));
output(reverseDidX8(did));
