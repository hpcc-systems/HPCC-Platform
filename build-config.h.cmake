#ifndef PREFIX
    #cmakedefine PREFIX "${PREFIX}"
#endif

#ifndef EXEC_PREFIX
    #cmakedefine EXEC_PREFIX "${EXEC_PREFIX}"
#endif

#ifndef CONFIG_PREFIX
    #cmakedefine CONFIG_PREFIX "${CONFIG_PREFIX}"
#endif

#ifndef DIR_NAME
    #cmakedefine DIR_NAME "${DIR_NAME}"
#endif

#ifndef INSTALL_DIR
    #define INSTALL_DIR "${INSTALL_DIR}"
#endif

#ifndef LIB_DIR
    #cmakedefine LIB_DIR "${LIB_PATH}"
#endif

#ifndef EXEC_DIR
    #cmakedefine EXEC_DIR "${EXEC_PATH}"
#endif

#ifndef COMPONENTFILES_DIR
    #cmakedefine COMPONENTFILES_DIR "${COMPONENTFILES_PATH}"
#endif

#ifndef CONFIG_DIR
    #define CONFIG_DIR "${CONFIG_DIR}"
#endif

#ifndef CONFIG_SOURCE_DIR
    #cmakedefine CONFIG_SOURCE_DIR "${CONFIG_SOURCE_PATH}"
#endif

#ifndef ADMIN_DIR
    #cmakedefine ADMIN_DIR "${ADMIN_PATH}"
#endif

#ifndef PLUGINS_DIR
    #cmakedefine PLUGINS_DIR "${PLUGINS_PATH}"
#endif

#ifndef RUNTIME_DIR
    #cmakedefine RUNTIME_DIR "${RUNTIME_PATH}"
#endif

#ifndef HOME_DIR
    #cmakedefine HOME_DIR "${HOME_DIR}"
#endif

#ifndef LOCK_DIR
    #cmakedefine LOCK_DIR "${LOCK_PATH}"
#endif

#ifndef PID_DIR
    #cmakedefine PID_DIR "${PID_PATH}"
#endif

#ifndef LOG_DIR
    #cmakedefine LOG_DIR "${LOG_PATH}"
#endif

#ifndef RUNTIME_USER
    #cmakedefine RUNTIME_USER "${RUNTIME_USER}"
#endif

#ifndef ENV_XML_FILE
    #cmakedefine ENV_XML_FILE "${ENV_XML_FILE}"
#endif

#ifndef ENV_CONF_FILE
    #cmakedefine ENV_CONF_FILE "${ENV_CONF_FILE}"
#endif

#ifndef BUILD_TAG
    #cmakedefine BUILD_TAG "${BUILD_TAG}"
#endif

#ifndef BUILD_VERSION_MAJOR
    #define BUILD_VERSION_MAJOR ${HPCC_MAJOR}
#endif

#ifndef BUILD_VERSION_MINOR
    #define BUILD_VERSION_MINOR ${HPCC_MINOR}
#endif

#ifndef BUILD_VERSION_POINT
    #define BUILD_VERSION_POINT ${HPCC_POINT}
#endif

#ifndef BASE_BUILD_TAG
    #cmakedefine BASE_BUILD_TAG "${BASE_BUILD_TAG}"
#endif

#ifndef BUILD_LEVEL
    #cmakedefine BUILD_LEVEL "${BUILD_LEVEL}"
#endif

#ifndef USE_RESOURCE
    #cmakedefine USE_RESOURCE "${USE_RESOURCE}"
#endif
