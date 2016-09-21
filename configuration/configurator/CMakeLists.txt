project ( configurator )

SET(  SRCS ConfiguratorMain.cpp ConfigFileComponentUtils.cpp ConfigFileUtils.cpp BuildSet.cpp ConfiguratorAPI.cpp EnvironmentModel.cpp JSONMarkUp.cpp
      ConfigFileUtilsObservable.cpp EnvironmentConfiguration.cpp WizardBase.cpp ConfigSchemaHelper.cpp ExceptionStrings.cpp DocumentationMarkup.hpp ConfigNotifications.cpp
      ./schemas/SchemaAttributes.cpp
      ./schemas/SchemaComplexType.cpp
      ./schemas/SchemaSequence.cpp
      ./schemas/SchemaElement.cpp
      ./schemas/SchemaAppInfo.cpp
      ./schemas/SchemaAnnotation.cpp
      ./schemas/SchemaComplexContent.cpp
      ./schemas/SchemaComplexType.cpp
      ./schemas/SchemaDocumentation.cpp
      ./schemas/SchemaExtension.cpp
      ./schemas/SchemaChoice.cpp
      ./schemas/SchemaInclude.cpp
      ./schemas/SchemaSchema.cpp
      ./schemas/SchemaSimpleType.cpp
      ./schemas/SchemaRestriction.cpp
      ./schemas/SchemaEnumeration.cpp
      ./schemas/SchemaCommon.cpp
      ./schemas/SchemaEnumeration.cpp
      ./schemas/SchemaAttributeGroup.cpp
      ./schemas/SchemaMapManager.cpp
      ./schemas/SchemaFractionDigits.cpp
      ./schemas/SchemaLength.cpp
      ./schemas/SchemaMaxExclusive.cpp
      ./schemas/SchemaMaxInclusive.cpp
      ./schemas/SchemaMaxLength.cpp
      ./schemas/SchemaMinInclusive.cpp
      ./schemas/SchemaMinExclusive.cpp
      ./schemas/SchemaMinLength.cpp
      ./schemas/SchemaPattern.cpp
      ./schemas/SchemaTotalDigits.cpp
      ./schemas/SchemaWhiteSpace.cpp
      ./schemas/SchemaKey.cpp
      ./schemas/SchemaKeyRef.cpp
      ./schemas/SchemaSelector.cpp
      ./schemas/SchemaUnique.cpp
      ./schemas/SchemaField.cpp
      ./schemas/SchemaSimpleContent.cpp
      )

  INCLUDE_DIRECTORIES(
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/deployment/deploy
        ${CMAKE_PREFIX_PATH}/mkspecs/linux-g++
        ${CMAKE_CURRENT_SOURCE_DIR}/configurator_ui
        ${CMAKE_CURRENT_SOURCE_DIR}/configurator_app
        ${CMAKE_CURRENT_SOURCE_DIR}/schemas
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_BINARY_DIR}
        ${CMAKE_BINARY_DIR}/oss
)

IF( CONFIGURATOR_LIB )
    MESSAGE( "-- configurator target is a library file" )

    ADD_DEFINITIONS( -D_USRDLL -DCONFIGURATOR_LIB)
    HPCC_ADD_LIBRARY( configurator SHARED ${SRCS} )
    TARGET_LINK_LIBRARIES( configurator jlib )

    INSTALL( TARGETS configurator LIBRARY DESTINATION ${LIB_DIR} )

ELSE ()
    MESSAGE( "-- configurator target is a binary executable" )

    ADD_DEFINITIONS( -D_CONSOLE )
    HPCC_ADD_EXECUTABLE( configurator  ${SRCS} )
    TARGET_LINK_LIBRARIES( configurator jlib )

    INSTALL( TARGETS configurator RUNTIME DESTINATION ${EXEC_DIR} )

ENDIF()
