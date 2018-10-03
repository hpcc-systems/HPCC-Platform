# Ubuntu 18.04
SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS g++ openssh-client openssh-server expect rsync libapr1 python psmisc curl )

if(SPARK)
    SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS "openjdk-8-jdk | openjdk-11-jdk" )
endif(SPARK)
