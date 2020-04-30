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
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_addrcleanlib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_bridgerscorelib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_canadacleanlib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_cmslib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_datalib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_date.eclmod DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_dmslib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_entityscorelib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_fingerprintlib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_hktlib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_namelib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_ofaclib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_ziplib.ecllib DESTINATION ${proxies_out_dir} COMPONENT Runtime)
