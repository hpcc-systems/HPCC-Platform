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

#ifndef __THORSTORE_HPP_
#define __THORSTORE_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

#include <vector>

//All of the following will move to a different location (?dali) once the proof of concept is completed.

//=====================================================================================================================

/*
 * File processing sequence:
 *
 * a) create a CLogicalFileCollection
 *   A) with an implicit superfile
 *      process each of the implicit subfiles in turn
 *      Do we allow logical files and
 *   B) with a logical super file
 *      process each of the subfiles in turn
 *   C) With a logical file
 *      - Resolve the file
 *      - extract the file information from dali
 *      - extract the split points (from dali/disk).
 *        potentially quite large, and expensive?  May want a hint to indicate how many ways the file will be read
 *        to avoid retrieving if there will be no benefit.
 *   D) With a absolute filename  [<host>|group?][:port]//path   (or local path if contains a /)
 *      - NOTE: ./abc can be used to refer to a physical file in the current directory
 *      - Add as a single (or multiple) part physical file, no logical distribution.
 *      - not sure if we should allow the port or not.  Could be used to force use of dafilesrv??
 *      - Need to check access rights - otherwise could extract security keys etc.
 *   E) As a URL transport-protocol://path
 *      - Expand wild cards (or other magic) to produce a full list of parts and nodes.  No logical distribution.
 *      - Retrieve meta information for the file
 *      - Retrieve partition information for the parts.
 *      - possibly allow thor:array//path as another representation for (D).  What would make sense?
 *   F) FILE::...
 *      Check access rights, then translate to same as (D)
 *   G) REMOTE::...
 *      Call into the remote esp to extract the file information and merge it with the location information
 *
 * b) perform protocol dependent processing
 *    - possibly part of stage (a) or a separate phase
 *    - passing #parts that it will be read from to allow split information to be optimized.
 *    A) thor
         - Translate logical nodes to physical ips.
         - gather any missing file sizes
      B) s3/azure blobs
         - Expand any wildcards in the filenames and create entries for each expanded file.
         - gather file sizes
      C) HDFS
         - gather files sizes
         - gather split points
 * c) serialize to the slaves
 * d) deserialize from the master
 * e) call fileCollection::partition(numChannels, myChannel) to get a list of partitions
 * f) iterate though the partitions for the current channel
 *    a) need a reader for the format - how determine?
 *    b) Where we determine if it should be read directly or via dafilesrv?
 *    c) request a row reader for the
 *
 * Questions:
 *    Where are the translators created?
 *    What is signed?  It needs to be a self contained block of data that can easily be serialized and deserialized.
 *      I don't think it needs to contain information about the storage array - only the logical file.
 */

class CStorageSystems;

//What is a good term for a collection of drives
//storage array/system/

//This describes a set of disks that can be used to store a logical file.
//  "device" is used to represent the index within the storage plane
class THORHELPER_API CStorageHostGroup : public CInterface
{
public:
    CStorageHostGroup(const IPropertyTree * _xml);

    const char * queryName() const;
    const char * queryHost(unsigned idx) const;
    bool isLocal(unsigned device) const;

private:
    const IPropertyTree * xml = nullptr;
};


//This describes the a set of disks that can be used to store a logical file.
//  "device" is used to represent the index within the storage plane
class THORHELPER_API CStoragePlane : public CInterface
{
    friend class CStorageSystems;

public:
    CStoragePlane() = default;
    CStoragePlane(const IPropertyTree * _xml, const CStorageHostGroup * _host);

    bool containsHost(const char * host) const;
    bool containsPath(const char * path);
    bool matches(const char * search) const { return strsame(name, search); }
    bool matchesHost(const char * host);

//    unsigned getCost(unsigned device, const IpAddress & accessIp, PhysicalGroup & peerIPs) const;
    StringBuffer & getURL(StringBuffer & target, unsigned part) const;
    unsigned getWidth() const;
    bool isLocal(unsigned part) const;
    bool isAttachedStorage() const;
    const char * queryName() const { return name; }
    const char * queryPath() const;
    const char * queryScopeSeparator() const;

protected:
    unsigned getNumDrives() const { return 1; }  // MORE: Should we allow support for drives as well as devices?
    unsigned getDevice(unsigned part) const;
    unsigned getDrive(unsigned part) const;

private:
    const IPropertyTree * xml = nullptr;
    const char * name = nullptr;
    const CStorageHostGroup * hostGroup = nullptr;
    unsigned numDevices = 0;
    unsigned offset = 0;  // offset + size <= plane->getWidth();
    unsigned size = 0;  // size <= plane->getWidth();
    unsigned startDelta = 0;  // allows different replication to be represented 0|1|width|....  Can be > size if plane has multiple drives
    unsigned startDrive = 0;
};

class CStorageSystems
{
public:
    void registerPlane(const IPropertyTree * _meta);
    void setFromMeta(const IPropertyTree * _meta);

    const CStorageHostGroup * queryHostGroup(const char * search) const;
    const CStoragePlane * queryPlane(const char * search) const;
    const CStoragePlane * queryNullPlane() const;

protected:
    CIArrayOf<CStorageHostGroup> hostGroups;
    CIArrayOf<CStoragePlane> planes;
    mutable Owned<IPropertyTree> nullPlaneXml;
    mutable Owned<CStoragePlane> nullPlane;
};

//Exapand the a string, substituting hashes for the device number.  Default number of digits matches the number of hashes
extern THORHELPER_API StringBuffer & expandPlanePath(StringBuffer & target, const char * path, unsigned device);

#endif
