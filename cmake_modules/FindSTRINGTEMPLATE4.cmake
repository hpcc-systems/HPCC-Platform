################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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
# - Attempt to find the STRINGTEMPLATE4 jar
# Once done this will define
#
# STRINGTEMPLATE4_FOUND - STRINGTEMPLATE4 found in local system
# STRINGTEMPLATE4_JAR - The jar needed for StringTemplate4
################################################################################

include(UseJava)

find_jar(STRINGTEMPLATE4_JAR stringtemplate4 PATHS /usr/share/java)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    STRINGTEMPLATE4 DEFAULT_MSG
    STRINGTEMPLATE4_JAR
    )
mark_as_advanced(STRINGTEMPLATE4_JAR)
