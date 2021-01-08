/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

//class=embedded
//class=embedded-js
//class=3rdparty

//nothor

//Thor doesn't handle CATCH properly, see HPCC-9059
//skip type==thorlcr TBD

IMPORT javascript;

integer testThrow(integer val) := EMBED(javascript) throw new Error("Error from JavaScript"); ENDEMBED;
// Test exception throwing/catching
d := dataset([{ 1, '' }], { integer a, string m} ) : stored('nofold');

d t := transform
  self.a := FAILCODE;
  self.m := FAILMESSAGE;
  self := [];
end;

catch(d(testThrow(a) = a), onfail(t));
