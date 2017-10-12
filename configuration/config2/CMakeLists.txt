project ( config2 )

SET(  SRCS 
      EnvironmentMgr.cpp
      XMLEnvironmentMgr.cpp
	  ConfigParser.cpp
	  ConfigItemValueSet.cpp
	  ConfigItem.cpp
	  CfgValue.cpp
	  EnvironmentNode.cpp
	  EnvValue.cpp
	  NodeStatus.cpp
	  XSDComponentParser.cpp
	  XSDConfigParser.cpp
	  XSDValueSetParser.cpp
)

INCLUDE_DIRECTORIES(
      ${HPCC_SOURCE_DIR}/system/include
      ${HPCC_SOURCE_DIR}/system/jlib
      ${HPCC_SOURCE_DIR}/deployment/deploy
      ${CMAKE_PREFIX_PATH}/mkspecs/linux-g++
      ${CMAKE_CURRENT_SOURCE_DIR}
      ${CMAKE_BINARY_DIR}
      ${CMAKE_BINARY_DIR}/oss
)

ADD_DEFINITIONS( -D_USRDLL -DCONFIG2_LIB)
HPCC_ADD_LIBRARY( config2 SHARED ${SRCS} )
TARGET_LINK_LIBRARIES( config2 jlib )

INSTALL( TARGETS config2 LIBRARY DESTINATION ${LIB_DIR} )

