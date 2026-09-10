/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#ifndef JHCONST_HPP
#define JHCONST_HPP

// This file only contains public constants that may be required by jlib and other parts of the system
// that this code is dependent on.  Extracting into a separate header helps avoid circular dependencies.
// This file should not include any others.

enum NodeType : unsigned char
{
    NodeBranch = 0,
    NodeLeaf = 1,
    NodeBlob = 2,
    NodeMeta = 3,
    NodeBloom = 4,

//The following is never stored and only used in code as a value that does not match any of the above.
    NodeNone = 7,
    NodeTypeMask = 0x07, // The above values are stored in the low 3 bits of the node kind field
};

enum NodeTypeWithFlags : unsigned
{
// The following flags are passed down in the node kind field to provide extra information to getCachedNode
// to help determine whether extra data should be read from an external file
    NodeNoFlags          = 0x00, // No special flags
    NodeSearchAllKeyed   = 0x08, // All keyed fields are provided when searching
    NodeSearchSingleValue= 0x10, // Only a single value is provided when searching
    NodeSearchUnfiltered = 0x20, // No filtering is applied - only postfiltering

// The following are specific to an index.  They are less useful for tracing, but may be useful in getCachedNode()
    NodeInTLK            = 0x40, // The node is part of a top-level key (TLK) - used to optimize loading of TLK nodes
    NodeInSinglePartFile = 0x80, // The node is part of a single-partition file
};

enum CompressionType : unsigned char
{
    LegacyCompression = 0,    // Keys built prior to 8.12.x will always have 0 here
    // Additional compression formats can be added here...
    SplitPayload = 1,               // A proof-of-concept using separate compression blocks for keyed fields vs payload
    InplaceCompression = 2,         // Inplace compression - used for hybrid branches and inplace leaves and branches.
    ExperimentalCompression = 3,    // Placeholder for testing new compression methods
    NewBlobCompression = 4,         // Blobs encoded with non-lzw compression
    BlockCompression = 5,           // Used for leaves in hybrid indexes
};

#endif
