cmake_minimum_required(VERSION 2.8)

set ( HPCC_ECLJDBC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set ( HPCC_SOURCE_DIR ${HPCC_ECLJDBC_SOURCE_DIR}/../../../../../../../../..)
include(${HPCC_SOURCE_DIR}/version.cmake)

configure_file("EclVersionTracker.java.in" "${HPCC_ECLJDBC_SOURCE_DIR}/EclVersionTracker.java")

set ( CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules")
set ( EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/bin" )
set ( PRODUCT_PREFIX "hpccsystems" )

SET (CLASS_DIR "class")
#SET (JAR_DIR "jar")
FILE(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CLASS_DIR})
#FILE(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${JAR_DIR})
SET (JAR_FILE "${PRODUCT_PREFIX}-ecljdbc_${HPCC_PROJECT}-${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}.jar")
SET (JAVA_FILES

EclDatabaseMetaData.java EclQuery.java SqlExpression.java DFUFile.java EclDriver.java EclResultSet.java SqlOperator.java EclColumn.java EclEngine.java EclResultSetMetadata.java SqlParser.java EclColumnMetaData.java EclPreparedStatement.java EclStatement.java SqlWhereClause.java EclConnection.java EclQueries.java SqlColumn.java Utils.java EclVersionTracker.java

)

# compile all .java files with javac to .class
ADD_CUSTOM_COMMAND(
OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${JAVA_FILES}.class
COMMAND ${CMAKE_Java_COMPILER}
#ARGS -d ${CMAKE_CURRENT_SOURCE_DIR}/${CLASS_DIR}
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
${CMAKE_Java_ARCHIVE} -cfv ${CMAKE_CURRENT_BINARY_DIR}/${JAR_FILE} com/hpccsystems/ecljdbc
)

# the target
ADD_CUSTOM_TARGET(
${JAR_FILE}
ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${JAR_FILE}
)
