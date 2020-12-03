/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef DAMETA_HPP
#define DAMETA_HPP

#ifdef DALI_EXPORTS
#define da_decl DECL_EXPORT
#else
#define da_decl DECL_IMPORT
#endif

#include "jptree.hpp"
#include "dasess.hpp"

enum ResolveOptions : unsigned {
    ROnone              = 0x00000000,
    ROincludeLocation   = 0x00000001,
    ROdiskinfo          = 0x00000002,       // disk sizes etc.
    ROpartinfo          = 0x00000004,
    ROtimestamps        = 0x00000008,
    ROsecrets           = 0x00000010,       // Needs careful thought to ensure they can't leak to the outside world.
    ROsizes             = 0x00000020,
    ROall               = ~0U
};

constexpr ResolveOptions operator |(ResolveOptions l, ResolveOptions r) { return (ResolveOptions)((unsigned)l | (unsigned)r); }
constexpr ResolveOptions operator &(ResolveOptions l, ResolveOptions r) { return (ResolveOptions)((unsigned)l & (unsigned)r); }

/*
 * Gather information about the logical filename from dali, checking that the user has access to the files.
 * Note, this will be serialized as a binary structure, and if signing is required that binary blob will then be signed.
 *
 * @param filename      The logical name of the file.  This can be
 *                        an implicit superfile {a,b,c}
 *                        a superfile
 *                        a logical filename
 *                        an external file:: file
 *                        an external plane:: file
 * @param user          Which user is requesting access to the file
 * @param options       A bitfield that controls which pieces of information are returned
 * @return              A property tree that can be used to generate the YAML to represent the file
 *
 */

extern da_decl IPropertyTree * resolveLogicalFilenameFromDali(const char * filename, IUserDescriptor * user, ResolveOptions options);

#endif
