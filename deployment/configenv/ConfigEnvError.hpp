/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef _CONFIGENVERROR_HPP_
#define _CONFIGENVERROR_HPP_

enum CfgEnvErrorCode
{
   OK=0,

   ComponentExists=1000,

   InvalidParams=2000,
   NullPointer=2001,
   UnknownCompoent=2002,
   CannotCreateCompoent=2003,
   UnknownTask=2004,

   InvalidIPRange=2100,
   InvalidIP=2101,
   NoIPAddress=2102,

   NonInteger=2200,
   OutOfRange=2201
};


#endif
