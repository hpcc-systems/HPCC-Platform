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

#ifndef DLLSERVEERR_HPP
#define DLLSERVEERR_HPP

#define ERR_DSV_FIRST                           9000
#define ERR_DSV_LAST                            9009

#define DSVERR_CouldNotFindDll                  9000
#define DSVERR_DllLibIpMismatch                 9001
#define DSVERR_NoAssociatedLib                  9002
#define DSVERR_LibNotLocal                      9003

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define DSVERR_CouldNotFindDll_Text             "Could not find location of %s"
#define DSVERR_DllLibIpMismatch_Text            "The DLL and Library have a mismatched IP address"
#define DSVERR_NoAssociatedLib_Text             "No Library associated with DLL %s"
#define DSVERR_LibNotLocal_Text                 "Library for %s not copied locally"

#endif
