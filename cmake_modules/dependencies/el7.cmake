# Centos 7
SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES m4 libtool gcc-c++ openssh-server openssh-clients expect rsync zip psmisc curl jq )

if(SPARK)
    SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES java-1.8.0-openjdk-devel )
endif(SPARK)
