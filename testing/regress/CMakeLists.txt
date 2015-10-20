################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    All rights reserved. This program is free software: you can redistribute it
#    and/or modify
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
HPCC_ADD_SUBDIRECTORY (hpcc)
HPCC_ADD_SUBDIRECTORY (ecl)

Install ( DIRECTORY download hpcc ecl DESTINATION "./testing/regress" COMPONENT Runtime
          USE_SOURCE_PERMISSIONS
          FILES_MATCHING
          PATTERN "hpcc/__init__.py"
          PATTERN "download/0drvb10.txt"
          PATTERN "download/donQuixote.txt"
          PATTERN "download/pge0112.txt"
          PATTERN "download/spray_test.txt"
          PATTERN "ecl/*.ecl"
          PATTERN "ecl/*.xml"
          PATTERN "ecl/*.queryxml" )
