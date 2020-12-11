################################################################################
#    Copyright (C) 2012 HPCC Systems®.
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
HPCC_ADD_SUBDIRECTORY (unittests)


Install ( DIRECTORY regress DESTINATION "./testing" COMPONENT Runtime
          USE_SOURCE_PERMISSIONS
          PATTERN "environment.xml.in" EXCLUDE
          PATTERN "ecl-test-cluster160.json" EXCLUDE
        )

configure_file("regress/environment.xml.in" "regress/environment.xml")

Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/regress/environment.xml DESTINATION "./testing/regress" COMPONENT Runtime )

