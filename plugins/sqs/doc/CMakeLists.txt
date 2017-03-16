find_package(Doxygen)
if(NOT DOXYGEN_FOUND)
    message("Doxygen not found, I will not generate/install the documentation")
else()
   configure_file(Doxyfile.in Doxyfile)

   set(DOXYGEN_INPUT ${CMAKE_BINARY_DIR}/plugins/sqs/doc/Doxyfile)
   set(DOXYGEN_OUTPUT ${APIDOC_DIR}/plugins/sqs/doc/html/index.html)

   add_custom_target(doc ALL
     COMMAND ${CMAKE_COMMAND} -E echo_append "Building API Documentation..."
     COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_INPUT} > /dev/null
     COMMAND ${CMAKE_COMMAND} -E echo "Done."
     )

  install(DIRECTORY ${CMAKE_BINARY_DIR}/plugins/sqs/doc/html 
          DESTINATION share/doc/${CMAKE_PROJECT_NAME})

endif()
