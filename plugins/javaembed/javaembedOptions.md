# Options for Embedded Java in HPCC

The embedded Java plugin is managed through several configuration options that control its runtime behavior. These settings allow you to customize how the plugin interacts with the Java Virtual Machine (JVM), manages native libraries, and locates Java classes and dependencies.

## Containerized Deployments

In containerized environments, the plugin is configured via environment variables.

- **JAVA_LIBRARY_PATH**
  - Sets the `-Djava.library.path` JVM option.
  - Specifies where the JVM should look for native libraries (such as `.so`, `.dll`, or `.dylib` files) used by Java code via the Java Native Interface (JNI).
- **JVM_OPTIONS**
  - Space-delimited list of JVM options appended to the JVM invocation.
  - Controls the behavior of the JVM when launched from ECL.
- **CLASSPATH**
  - List of directories, JAR files, or ZIP files that the JVM searches when loading classes not included in the Java standard library.
  - Default: The default value is the classes sub-folder under the install directory.
  - Useful for specifying the location of target Java classes and their dependencies.
- **HPCC_JAVA_EMBED_LOG4J_LEVEL**
  - Sets the log4j log scope level. Required to enable Java log4j redirection
  - Available options: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL
  - See: https://logging.apache.org/log4j/2.x/manual/customloglevels.html
- **HPCC_JAVA_EMBED_LOG4J_PATTERN**
  - Sets the log message pattern for Java log4j.
  - Optional (Default: [%t] - %msg%n)
  - **\*WARNING\*** An invalid pattern will cause log4j to silently fail to redirect logs to HPCC
  - See: https://logging.apache.org/log4j/2.x/manual/pattern-layout.html

In containerized environments, you can only use Java versions included in the HPCC Systems-provided image.  
To use a different Java version or distribution, you must customize the base Docker image and set the above configuration options accordingly.

## Bare-Metal Deployments

In bare-metal systems, administrators can install or remove Java environments as needed and configure the HPCC platform's embedded Java plugin to use the desired Java version by updating the relevant settings in `environment.conf`.

- **jvmlibpath**
  - Sets `-Djava.library.path=`.
  - Specifies where the JVM should look for native libraries used by Java code via JNI.
- **jvmoptions**
  - Space-delimited list of JVM options appended to the JVM invocation.
- **classpath**
  - List of directories, JAR files, or ZIP files that the JVM searches when loading classes not included in the Java standard library.
  - Useful for specifying the location of target Java classes and their dependencies.
- **JNI_PATH**
  - Used if HPCC cannot locate the appropriate JNI library (`libjvm`) or if you want to specify an alternative `libjvm`.
  - Example:
- **log4jLevel**
  - Sets the log4j log scope level. Required to enable Java log4j redirection
  - Available options: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL
  - See: https://logging.apache.org/log4j/2.x/manual/customloglevels.html
- **log4jPattern**
  - Sets the log message pattern for Java log4j.
  - Optional (Default: [%t] - %msg%n)
  - **\*WARNING\*** An invalid pattern will cause log4j to silently fail to redirect logs to HPCC
  - See: https://logging.apache.org/log4j/2.x/manual/pattern-layout.html

    ```text
    JNI_PATH=/absolute/path/to/alternative/libjvm.so
    ```
