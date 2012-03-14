project(docbook)
ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/docbook-xsl.sentinel
        COMMAND ${CMAKE_COMMAND} -E tar xjf     ${CMAKE_CURRENT_SOURCE_DIR}/docbook-xsl.tar.bz2
	COMMAND ${CMAKE_COMMAND} -E copy        ${CMAKE_CURRENT_SOURCE_DIR}/../BuildTools/pagesetup.xsl ${CMAKE_CURRENT_BINARY_DIR}/docbook-xsl/fo/pagesetup.xsl
        COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/docbook-xsl.sentinel
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/docbook-xsl.tar.bz2
        WORKING_DIRECTORY       ${CMAKE_CURRENT_BINARY_DIR}
        )
ADD_CUSTOM_TARGET(docbook-xsl-expand DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/docbook-xsl.sentinel)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/docbook-xml.sentinel
        COMMAND ${CMAKE_COMMAND} -E tar xjf     ${CMAKE_CURRENT_SOURCE_DIR}/docbook-xml-4.5.tar.bz2
        COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/docbook-xml.sentinel
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/docbook-xml-4.5.tar.bz2
        WORKING_DIRECTORY       ${CMAKE_CURRENT_BINARY_DIR}
        )
ADD_CUSTOM_TARGET(docbook-xml-expand DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/docbook-xml.sentinel)

ADD_CUSTOM_TARGET(docbook-expand DEPENDS docbook-xml-expand docbook-xsl-expand)
