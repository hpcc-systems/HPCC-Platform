################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

# Component: density

#####################################################
# Description:
# ------------
#    Cmake Input File for density
#####################################################


project( density )

set ( SRCS
        globals.c
        buffers/buffer.c
        structure/header.c
        structure/footer.c
        streams/stream.c
        core/cheetah/cheetah_encode.c
        core/cheetah/cheetah_decode.c
        core/cheetah/cheetah_dictionary.c
        core/lion/lion_dictionary.c
        core/lion/lion_decode.c
        core/lion/lion_form_model.c
        core/lion/lion_encode.c
        core/chameleon/chameleon_decode.c
        core/chameleon/chameleon_encode.c
        core/chameleon/chameleon_dictionary.c
)

include_directories (
        ../../system/spookyhash
    )

ADD_DEFINITIONS( -D_LIB )

SET_SOURCE_FILES_PROPERTIES( ${SRCS} PROPERTIES LANGUAGE C )

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -std=c99")
endif()

HPCC_ADD_LIBRARY( density STATIC ${SRCS} )
