################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
add_subdirectory (auditlib)
add_subdirectory (debugservices)
add_subdirectory (fileservices)
add_subdirectory (logging)
add_subdirectory (parselib)
add_subdirectory (stringlib)
add_subdirectory (unicodelib)
add_subdirectory (workunitservices)
if ("${BUILD_LEVEL}" STREQUAL "COMMUNITY")
  add_subdirectory (proxies)
endif ()

if (USE_V8)
  add_subdirectory (v8embed)
endif ()
if (USE_PYTHON)
  add_subdirectory (pyembed)
endif ()
if (USE_JNI)
  add_subdirectory (javaembed)
endif()
if (USE_RINSIDE)
  add_subdirectory (Rembed)
endif()