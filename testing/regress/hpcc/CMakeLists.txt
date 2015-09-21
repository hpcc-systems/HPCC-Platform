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
HPCC_ADD_SUBDIRECTORY (util)
Install ( DIRECTORY common regression util DESTINATION "./testing/regress/hpcc" COMPONENT Runtime
          USE_SOURCE_PERMISSIONS
          FILES_MATCHING
          PATTERN "common/config.py"
          PATTERN "common/dict.py"
          PATTERN "common/error.py"
          PATTERN "common/__init__.py"
          PATTERN "common/logger.py"
          PATTERN "common/report.py"
          PATTERN "common/shell.py"
          PATTERN "regression/__init__.py"
          PATTERN "regression/regress.py"
          PATTERN "regression/suite.py"
          PATTERN "util/argparse.py"
          PATTERN "util/collections.py"
          PATTERN "util/configgen.py"
          PATTERN "util/expandcheck.py"
          PATTERN "util/__init__.py"
          PATTERN "util/util.py"
)
