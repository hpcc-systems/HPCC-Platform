cmake_minimum_required(VERSION 2.8)

configure_file("HPCCVersionTracker.java.in" "${HPCC_ECLJDBC_SOURCE_DIR}/HPCCVersionTracker.java")

set ( CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules")
set ( EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/bin" )
set ( PRODUCT_PREFIX "hpccsystems" )

SET (CLASS_DIR "class")
FILE(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CLASS_DIR})
SET (JAR_FILE "${PRODUCT_PREFIX}-${PROJECT_NAME}_${HPCC_PROJECT}-${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}.jar")
SET (JAVA_FILES
HPCCDatabaseMetaData.java HPCCQuery.java SQLExpression.java DFUFile.java HPCCDriver.java HPCCResultSet.java SQLOperator.java HPCCColumn.java ECLEngine.java HPCCResultSetMetadata.java SQLParser.java HPCCColumnMetaData.java HPCCPreparedStatement.java HPCCStatement.java SQLWhereClause.java HPCCConnection.java HPCCQueries.java SQLColumn.java HPCCJDBCUtils.java ECLFunction.java ECLFunctions.java HPCCVersionTracker.java 

)

# compile all .java files with javac to .class
ADD_CUSTOM_COMMAND(
OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${JAVA_FILES}.class
COMMAND ${CMAKE_Java_COMPILER}
ARGS -d ${CMAKE_CURRENT_BINARY_DIR}/${CLASS_DIR}
${CMAKE_CURRENT_SOURCE_DIR}/*.java
)

# build .jar file from .class files
ADD_CUSTOM_COMMAND(
OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${JAR_FILE}
DEPENDS
${CMAKE_CURRENT_BINARY_DIR}/${JAVA_FILES}.class
COMMAND ${CMAKE_COMMAND}
ARGS -E chdir ${CMAKE_CURRENT_BINARY_DIR}/${CLASS_DIR}
${CMAKE_Java_ARCHIVE} -cfv ${CMAKE_CURRENT_BINARY_DIR}/${JAR_FILE} ${HPCC_JDBC_PAC_PATH}
)

# the target
ADD_CUSTOM_TARGET(
${JAR_FILE}
ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${JAR_FILE}
)
