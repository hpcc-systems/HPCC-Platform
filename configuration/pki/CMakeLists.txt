# Developer build
if(NOT "${DESTDIR}" STREQUAL "")
  install(CODE "
  if(EXISTS ${INSTALL_DIR}/etc/init.d/setupPKI)
      execute_process(COMMAND ${INSTALL_DIR}/etc/init.d/setupPKI)
  endif()
  ")
endif()
