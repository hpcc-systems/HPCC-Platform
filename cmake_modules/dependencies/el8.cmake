# Centos 8
SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES m4 libtool gcc-c++ openssh-server openssh-clients expect rsync zip psmisc curl )

if(SPARK)
    SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES java-1.8.0-openjdk-devel )
endif(SPARK)
