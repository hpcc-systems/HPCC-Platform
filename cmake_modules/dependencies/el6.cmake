# Centos 6
SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES m4 libtool gcc-c++ openssh-server openssh-clients expect rsync zip psmisc devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ curl )

if(SPARK)
    SET_DEPENDENCIES ( CPACK_RPM_PACKAGE_REQUIRES java-1.8.0-openjdk-devel )
endif(SPARK)
