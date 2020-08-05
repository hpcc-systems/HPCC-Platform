# Ubuntu 20.04
SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS g++ openssh-client openssh-server expect rsync libapr1 python2 python3 psmisc curl )

if(SPARK)
    SET_DEPENDENCIES ( CPACK_DEBIAN_PACKAGE_DEPENDS "openjdk-11-jdk" )
endif(SPARK)
