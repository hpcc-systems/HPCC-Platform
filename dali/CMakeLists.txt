################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
HPCC_ADD_SUBDIRECTORY (base)
HPCC_ADD_SUBDIRECTORY (dalidiag "PLATFORM")
HPCC_ADD_SUBDIRECTORY (dalistop "PLATFORM")
HPCC_ADD_SUBDIRECTORY (datest "PLATFORM")
if (USE_CPPUNIT)
    HPCC_ADD_SUBDIRECTORY (daunittest "PLATFORM")
endif()
HPCC_ADD_SUBDIRECTORY (dfu "PLATFORM")
HPCC_ADD_SUBDIRECTORY (dfuplus "CLIENTTOOLS")
HPCC_ADD_SUBDIRECTORY (dfuxref "PLATFORM")
HPCC_ADD_SUBDIRECTORY (dfuXRefLib "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ft)
HPCC_ADD_SUBDIRECTORY (daliadmin "PLATFORM")
HPCC_ADD_SUBDIRECTORY (server "PLATFORM")
HPCC_ADD_SUBDIRECTORY (treeview "PLATFORM")
HPCC_ADD_SUBDIRECTORY (updtdalienv "PLATFORM")
HPCC_ADD_SUBDIRECTORY (sasha "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ldap "PLATFORM")
