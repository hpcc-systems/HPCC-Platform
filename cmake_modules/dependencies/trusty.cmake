# Ubuntu 14.04
SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS g++ openssh-client openssh-server expect rsync  zip python psmisc curl )

# Requires third party repository
if(SPARK)
    SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS "openjdk-8-jdk" )
endif(SPARK)
