###############################################################################
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
set (proxies_out_dir "plugins")
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_saltlib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_keylib.ecllib DESTINATION  ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_metaphone.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_metaphone3.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_thorlib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_word.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)

IF (USE_OPENSSL)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_cryptolib.ecllib DESTINATION plugins COMPONENT Runtime)
ENDIF()
