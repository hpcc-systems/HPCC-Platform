project(xsl-config)

configure_file(fo.xsl.in ${CMAKE_CURRENT_BINARY_DIR}/fo.xsl)
configure_file(catalog.xml.in ${CMAKE_CURRENT_BINARY_DIR}/catalog.xml)
configure_file(EclipseHelp.xsl.in ${CMAKE_CURRENT_BINARY_DIR}/EclipseHelp.xsl)
