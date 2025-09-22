# JavaEmbedNativeLogging Build Documentation

## Overview
This document describes the compilation process for `javaembedNativeLogging.java` into JAR and class files for use with HPCC Systems' Java Embedded Plugin.

## Source Files Structure
```
testing/regress/ecl/
├── javaembedNativeLogging.java      # Main Java source file
├── javaembedNativeLogging.ecl       # ECL test script
├── javaembedNativeLogging.manifest  # Resource manifest for HPCC
└── javaembedNativeLoggingPOM.xml    # Maven build configuration
```

##  Maven Build
The jar file can be built using Maven with the provided POM configuration.

**Dependencies:**
- Log4j Core 2.20.0
- Log4j API 2.20.0
- Java 17 (configurable)
- Apache Maven 3.8.7

**Build Commands:**
```bash
# (Recommended) Copy sources to external build directory
mkdir buildJavaEmbedNativeLogging &&
cp HPCC-Platform/testing/regress/ecl/javaembedNativeLogging* ./buildJavaEmbedNativeLogging &&
cd buildJavaEmbedNativeLogging &&

# Copy java source code to src/main/java/test (configured by groupid in pom.xml)
mkdir -p src/main/java/test &&
cp javaembedNativeLogging.java src/main/java/test &&

# Compile source code
mvn clean compile assembly:single -f javaembedNativeLoggingPOM.xml

# Output locations:
# - target/classes/javaembedNativeLogging.class
# - target/javaembedNativeLogging-1.0.0-jar-with-dependencies.jar
```

**Maven Configuration Features:**
- Assembly plugin creates fat JAR with all dependencies
- Compiler plugin ensures proper Java version targeting (Avoids pre-intalled java runtime complaining about too new of version in some installations)
- Targets Java 17

## Integration with HPCC

### Manifest Configuration
The `.manifest` file declares the JAR resource so it can be included in the workunit:
```xml
<Manifest>
 <Resource type='jar' filename='javaembedNativeLogging.jar'/>
</Manifest>
```

### ECL Integration
The ECL script imports the Java function:
```ecl
STRING testLogging(STRING message) := IMPORT(java, 'javaembedNativeLogging.testLogging:(Ljava/lang/String;)Ljava/lang/String;');
```

### Deployment Considerations
- Use fat JAR to avoid classpath dependency issues
- Configure Log4j settings 
  - via environment.conf (Bare-metal)
    - `log4jLevel`
    - `log4jPattern`
  - via HPCC environment variables (Containerized):
    - `HPCC_JAVA_EMBED_LOG4J_LEVEL`
    - `HPCC_JAVA_EMBED_LOG4J_PATTERN`
