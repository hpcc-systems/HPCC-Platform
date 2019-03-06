/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.

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
//class=3rdparty

import java;

major := 1;
minor := 0;
point := 1;

String getVersion(integer a, integer b, integer c) := EMBED(Java: FOLD)
String getVersion(int a, int b, int c) 
{
  return Integer.toString(a) + '.' + Integer.toString(b) + '.' + Integer.toString(c);
}
ENDEMBED;

#if (getVersion(major, minor, point)='1.0.0')
  OUTPUT('Version is too old')
#else
  OUTPUT('Version is good');
#end
