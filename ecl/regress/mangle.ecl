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



#declare(s1,s2)

#set(s1,'1$F')
#set(s2,#mangle(%'s1'%))

r1 := %'s2'%;

#set(s1,#demangle(%'s2'%))
r2 := %'s1'%;

#set(s1,'A1_F')
#set(s2,#mangle(%'s1'%))

r3 := %'s2'%;

#set(s1,#demangle(%'s2'%))
r4 := %'s1'%;
