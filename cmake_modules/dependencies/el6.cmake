# Centos 6
SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES m4 libtool openssh-server openssh-clients expect rsync zip psmisc devtoolset-7-gcc devtoolset-7-binutils devtoolset-7-gcc-c++ curl )

if(SPARK)
    SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES java-1.8.0-openjdk-devel )
endif(SPARK)
