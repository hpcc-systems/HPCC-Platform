/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#include <vector>

#include "jliball.hpp"
#include "jcontainerized.hpp"
#include "jflz.hpp"
#include "jsecrets.hpp"
#include "seclib.hpp"
#include "ws_dfs.hpp"
#include "workunit.hpp"

#include "eclwatch_errorlist.hpp" // only for ECLWATCH_FILE_NOT_EXIST
#include "soapmessage.hpp"
#include "soapbind.hpp"

#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#ifndef _CONTAINERIZED
#include "environment.hpp"
#endif

#include "ws_dfsclient.hpp"

namespace wsdfs
{

class CKeepAliveThread : public CSimpleInterface, implements IThreaded
{
    CThreaded threaded;
    unsigned periodMs;
    Semaphore sem;
public:
    CKeepAliveThread(unsigned _periodSecs) : threaded("CKeepAliveThread", this), periodMs(_periodSecs * 1000)
    {
        threaded.start(false);
    }
    void stop()
    {
        sem.signal();
    }
    virtual void threadmain() override
    {
        while (true)
        {
            if (sem.wait(periodMs))
                return;
        }
    }
};


template <class INTERFACE>
class CServiceDistributedFileBase : public CSimpleInterfaceOf<INTERFACE>
{
protected:
    Linked<IDFSFile> dfsFile;
    StringAttr logicalName;
    Owned<IDistributedFile> legacyDFSFile;
    Owned<IFileDescriptor> fileDesc;

    class CDistributedSuperFileIterator: public CSimpleInterfaceOf<IDistributedSuperFileIterator>
    {
        Linked<IDFSFile> source;
        Owned<IDistributedSuperFile> cur;
        std::vector<std::string> owners;
        unsigned which = 0;
        AccessMode accessMode = AccessMode::none;

        void setCurrent(unsigned w)
        {
            VStringBuffer lfn("~remote::%s::%s", source->queryRemoteName(), owners[w].c_str());
            Owned<IDFSFile> dfsFile = lookupDFSFile(lfn, accessMode, source->queryTimeoutSecs(), keepAliveExpiryFrequency, source->queryUserDescriptor());
            if (!dfsFile)
                throw makeStringExceptionV(0, "Failed to open superfile %s", lfn.str());
            if (!dfsFile->numSubFiles())
                throwUnexpected();
            Owned<IDistributedFile> legacyDFSFile = createLegacyDFSFile(dfsFile);
            IDistributedSuperFile *super = legacyDFSFile->querySuperFile();
            assertex(super);
            cur.set(super);
        }
    public:
        CDistributedSuperFileIterator(IDFSFile *_source, std::vector<std::string> _owners) : source(_source), owners(_owners)
        {
            accessMode = static_cast<AccessMode>(source->queryCommonMeta()->getPropInt("@accessMode"));
        }
        virtual bool first() override
        {
            if (owners.empty())
                return false;
            which = 0;
            setCurrent(which);
            return true;
        }
        virtual bool next() override
        {
            if (which == (owners.size()-1))
            {
                cur.clear();
                return false;
            }
            ++which;
            setCurrent(which);
            return true;
        }
        virtual bool isValid() override
        {
            return cur != nullptr;
        }
        virtual IDistributedSuperFile &query() override
        {
            return *cur;
        }
        virtual const char *queryName() override
        {
            if (!isValid())
                return nullptr;
            return owners[which].c_str();
        }
    };

public:
    CServiceDistributedFileBase(IDFSFile *_dfsFile) : dfsFile(_dfsFile)
    {
        logicalName.set(dfsFile->queryFileMeta()->queryProp("@name"));
    }

    virtual unsigned numParts() override { return legacyDFSFile->numParts(); }
    virtual IDistributedFilePart &queryPart(unsigned idx) override { return legacyDFSFile->queryPart(idx); }
    virtual IDistributedFilePart* getPart(unsigned idx) override { return legacyDFSFile->getPart(idx); }
    virtual StringBuffer &getLogicalName(StringBuffer &name) override { return legacyDFSFile->getLogicalName(name); }
    virtual const char *queryLogicalName() override { return legacyDFSFile->queryLogicalName(); }
    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) override { return legacyDFSFile->getIterator(filter); }
    virtual IFileDescriptor *getFileDescriptor(const char *clustername=NULL) override { return fileDesc.getLink(); }
    virtual const char *queryDefaultDir() override { return legacyDFSFile->queryDefaultDir(); }
    virtual const char *queryPartMask() override { return legacyDFSFile->queryPartMask(); }
    virtual IPropertyTree &queryAttributes() override { return legacyDFSFile->queryAttributes(); }
    virtual bool lockProperties(unsigned timeoutms=INFINITE) override
    {
        // TODO: implement. But for now only foreign [read] files are supported, where updates and locking have never been implemented.
        return true;
    }
    virtual void unlockProperties(DFTransactionState state=TAS_NONE) override
    {
        // TODO: implement. But for now only foreign [read] files are supported, where updates and locking have never been implemented.
    }
    virtual bool getModificationTime(CDateTime &dt) override { return legacyDFSFile->getModificationTime(dt); }
    virtual bool getAccessedTime(CDateTime &dt) override { return legacyDFSFile->getAccessedTime(dt); }
    virtual unsigned numCopies(unsigned partno) override { return legacyDFSFile->numCopies(partno); }
    virtual bool existsPhysicalPartFiles(unsigned short port) override
    {
        return legacyDFSFile->existsPhysicalPartFiles(port);
    }
    virtual __int64 getFileSize(bool allowphysical, bool forcephysical) override
    {
        return legacyDFSFile->getFileSize(allowphysical, forcephysical);
    }
    virtual __int64 getDiskSize(bool allowphysical, bool forcephysical) override
    {
        return legacyDFSFile->getDiskSize(allowphysical, forcephysical);
    }
    virtual bool getFileCheckSum(unsigned &checksum) override { return legacyDFSFile->getFileCheckSum(checksum); }
    virtual unsigned getPositionPart(offset_t pos,offset_t &base) override { return legacyDFSFile->getPositionPart(pos,base); }
    virtual IDistributedSuperFile *querySuperFile() override
    {
        return nullptr;
    }
    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *transaction=NULL) override
    {
        if (transaction)
            throwUnexpected();
        Owned<IPropertyTreeIterator> iter = dfsFile->queryFileMeta()->getElements("SuperFile/SuperOwner");
        std::vector<std::string> superOwners;
        StringBuffer pname;
        ForEach(*iter)
        {
            iter->query().getProp("@name",pname.clear());
            if (pname.length())
                superOwners.push_back(pname.str());
        }

        return new CDistributedSuperFileIterator(dfsFile, superOwners);
    }
    virtual bool isCompressed(bool *blocked=NULL) override { return legacyDFSFile->isCompressed(blocked); }
    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) override { return legacyDFSFile->getClusterName(clusternum,name); }
    virtual unsigned getClusterNames(StringArray &clusters) override { return legacyDFSFile->getClusterNames(clusters); }                                                                                      // (use findCluster)
    virtual unsigned numClusters() override { return legacyDFSFile->numClusters(); }
    virtual unsigned findCluster(const char *clustername) override { return legacyDFSFile->findCluster(clustername); }
    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum) override { return legacyDFSFile->queryPartDiskMapping(clusternum); }
    virtual IGroup *queryClusterGroup(unsigned clusternum) override { return legacyDFSFile->queryClusterGroup(clusternum); }
    virtual StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name) override
    {
        return fileDesc->getClusterGroupName(clusternum, name);
    }
    virtual StringBuffer &getECL(StringBuffer &buf) override { return legacyDFSFile->getECL(buf); }

    virtual bool canModify(StringBuffer &reason) override
    {
        return false;
    }
    virtual bool canRemove(StringBuffer &reason,bool ignoresub=false) override
    {
        return false;
    }
    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) override { return legacyDFSFile->checkClusterCompatible(fdesc,err); }

    virtual bool getFormatCrc(unsigned &crc) override { return legacyDFSFile->getFormatCrc(crc); }
    virtual bool getRecordSize(size32_t &rsz) override { return legacyDFSFile->getRecordSize(rsz); }
    virtual bool getRecordLayout(MemoryBuffer &layout, const char *attrname) override { return legacyDFSFile->getRecordLayout(layout,attrname); }
    virtual StringBuffer &getColumnMapping(StringBuffer &mapping) override { return legacyDFSFile->getColumnMapping(mapping); }

    virtual bool isRestrictedAccess() override { return legacyDFSFile->isRestrictedAccess(); }
    virtual unsigned setDefaultTimeout(unsigned timems) override { return legacyDFSFile->setDefaultTimeout(timems); }

    virtual void validate() override { legacyDFSFile->validate(); }

    virtual IPropertyTree *queryHistory() const override { return legacyDFSFile->queryHistory(); }
    virtual bool isExternal() const override { return false; }
    virtual bool getSkewInfo(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart, bool calculateIfMissing) override { return legacyDFSFile->getSkewInfo(maxSkew, minSkew, maxSkewPart, minSkewPart, calculateIfMissing); }
    virtual int getExpire(StringBuffer *expirationDate) override { return legacyDFSFile->getExpire(expirationDate); }
    virtual void getCost(const char * cluster, cost_type & atRestCost, cost_type & accessCost) override { legacyDFSFile->getCost(cluster, atRestCost, accessCost); }
    
// setters (change file meta data)
    virtual void setPreferredClusters(const char *clusters) override { legacyDFSFile->setPreferredClusters(clusters); }
    virtual void setSingleClusterOnly() override { legacyDFSFile->setSingleClusterOnly(); }
    virtual void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec) override { legacyDFSFile->addCluster(clustername, mspec); }
    virtual bool removeCluster(const char *clustername) override { return legacyDFSFile->removeCluster(clustername); }
    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec) override { legacyDFSFile->updatePartDiskMapping(clustername, spec); }

/* NB/TBD: these modifications are only effecting this instance, the changes are not propagated to Dali
 * This is the same behaviour when foreign files are used, but will need addressing in future.
 */
    virtual void setModificationTime(const CDateTime &dt) override
    {
        legacyDFSFile->setModificationTime(dt);
    }
    virtual void setModified() override
    {
        legacyDFSFile->setModified();
    }
    virtual void setAccessedTime(const CDateTime &dt) override
    {
        legacyDFSFile->setAccessedTime(dt);
    }
    virtual void setAccessed() override
    {
        legacyDFSFile->setAccessed();
    }
    virtual void addAttrValue(const char *attr, unsigned __int64 value) override
    {
        legacyDFSFile->addAttrValue(attr, value);
    }
    virtual void setExpire(int expireDays) override
    {
        legacyDFSFile->setExpire(expireDays);
    }
    virtual void setECL(const char *ecl) override
    {
        legacyDFSFile->setECL(ecl);
    }
    virtual void resetHistory() override
    {
        legacyDFSFile->resetHistory();
    }
    virtual void setProtect(const char *callerid, bool protect=true, unsigned timeoutms=INFINITE) override
    {
        legacyDFSFile->setProtect(callerid, protect, timeoutms);
    }
    virtual void setColumnMapping(const char *mapping) override
    {
        legacyDFSFile->setColumnMapping(mapping);
    }
    virtual void setRestrictedAccess(bool restricted) override
    {
        legacyDFSFile->setRestrictedAccess(restricted);
    }
    virtual bool renamePhysicalPartFiles(const char *newlfn,const char *cluster=NULL,IMultiException *exceptions=NULL,const char *newbasedir=NULL) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::renamePhysicalPartFiles");
    }
    virtual void rename(const char *logicalname,IUserDescriptor *user) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::rename");
    }
    virtual void attach(const char *logicalname,IUserDescriptor *user) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::rename");
    }
    virtual void detach(unsigned timeoutms=INFINITE, ICodeContext *ctx=NULL) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::detach");
    }
    virtual void enqueueReplicate() override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::enqueueReplicate");
    }
};

class CServiceDistributedFile : public CServiceDistributedFileBase<IDistributedFile>
{
    typedef CServiceDistributedFileBase<IDistributedFile> PARENT;
public:
    CServiceDistributedFile(IDFSFile *_dfsFile) : PARENT(_dfsFile)
    {
        IPropertyTree *file = dfsFile->queryFileMeta()->queryPropTree("File");

        const char *remoteName = dfsFile->queryRemoteName(); // NB: null if local
        if (!isEmptyString(remoteName))
        {
            IPropertyTree *dafileSrvRemoteFilePlane = nullptr;
            Owned<IPropertyTree> remoteStorage = getRemoteStorage(remoteName);
            if (!remoteStorage)
                throw makeStringExceptionV(0, "Remote storage '%s' not found", remoteName);
            const char *remotePlaneName = file->queryProp("@group");
            VStringBuffer planeXPath("planes[@name=\"%s\"]", remotePlaneName);
            IPropertyTree *filePlane = dfsFile->queryCommonMeta()->queryPropTree(planeXPath);
            assertex(filePlane);
            if (remoteStorage->getPropBool("@useDafilesrv"))
            {
                // Some info needs to be propagated down to the workers and then used by dafilesrv connections.
                // Since the workers are currently only serialized IPartDescriptor/IFileDescriptors,
                // this info. is conveyed via attributes in the IFileDescriptor.

                file->setPropTree("Attr/_remoteStoragePlane", createPTreeFromIPT(filePlane));

                const char *serviceUrl = remoteStorage->queryProp("@service");
                if (serviceUrl && startsWith(serviceUrl, "https"))
                {
                    // if remote storage service is secure, dafilesrv connections must be also.
                    // this flag is used by consumers of this IFleDescriptor to tell whether they need to make
                    // secure connections to the dafilesrv's
                    file->setPropBool("Attr/@_remoteSecure", true);

                    // Propagate whether the remote definition has supplied a explicit secret (if not it will be auto-generated based on service URLs)
                    if (remoteStorage->hasProp("@secret"))
                    {
                        const char *secret = remoteStorage->queryProp("@secret");
                        if (isEmptyString(secret)) // i.e. no secret, meaning TLS only.
                            secret = "<TLS>";  // placeholder, picked up by dafilesrv at connect time when determining if TLS or secret based

                        file->setProp("Attr/@_remoteSecret", secret);
                    }
                }
            }
            else
            {
                // Path translation is necessary, because the local plane will not necessarily have the same
                // prefix. In particular, both a local and remote plane may want to use the same prefix/mount.
                // So, the local plane will be defined with a unique prefix locally.
                // Files backed by URL's or hostGroups will be access directly, are not mounted, and do not require
                // this translation.

                const char *filePlanePrefix = filePlane->queryProp("@prefix");
                if (isAbsolutePath(filePlanePrefix) && !filePlane->hasProp("hosts")) // otherwise assume url
                {
#ifndef _CONTAINERIZED
                    throw makeStringException(0, "Bare metal does not support remote file access to planes without hosts");
#endif
                    // A external plane within another environment backed by a PVC, will need a pre-existing
                    // corresponding plane and PVC in the local environment.
                    // The local plane will be associated with the remote environment, via a storage/remote mapping.

                    VStringBuffer remotePlaneXPath("planes[@remote='%s']/@local", remotePlaneName);
                    const char *localMappedPlaneName = remoteStorage->queryProp(remotePlaneXPath);
                    if (isEmptyString(localMappedPlaneName))
                        throw makeStringExceptionV(0, "Remote plane '%s' not found in remote storage definition '%s'", remotePlaneName, remoteName);

                    Owned<IStoragePlane> localPlane = getRemoteStoragePlane(localMappedPlaneName, false);
                    if (!localPlane)
                        throw makeStringExceptionV(0, "Local plane not found, mapped to by remote storage '%s' (%s->%s)", remoteName, remotePlaneName, localMappedPlaneName);

                    DBGLOG("Remote logical file '%s' using remote storage '%s', mapping remote plane '%s' to local plane '%s'", logicalName.str(), remoteName, remotePlaneName, localMappedPlaneName);

                    StringBuffer filePlanePrefix;
                    filePlane->getProp("@prefix", filePlanePrefix);
                    if (filePlane->hasProp("@subPath"))
                        filePlanePrefix.append('/').append(filePlane->queryProp("@subPath"));

                    // the plane prefix should match the base of file's base directory
                    // Q: what if the plane has been redefined since the files were created?

                    VStringBuffer clusterXPath("Cluster[@name=\"%s\"]", remotePlaneName);
                    IPropertyTree *cluster = file->queryPropTree(clusterXPath);
                    assertex(cluster);
                    const char *clusterDir = cluster->queryProp("@defaultBaseDir");
                    assertex(startsWith(clusterDir, filePlanePrefix));
                    clusterDir += filePlanePrefix.length();
                    StringBuffer newPath(localPlane->queryPrefix());
                    if (strlen(clusterDir))
                        newPath.append(clusterDir); // add remaining tail of path
                    cluster->setProp("@defaultBaseDir", newPath.str());

                    // need to rename Cluster/@name too, because local plane will be referenced e.g. for numStripedDevices
                    cluster->setProp("@name", localMappedPlaneName);

                    const char *dir = file->queryProp("@directory");
                    assertex(startsWith(dir, filePlanePrefix));
                    dir += filePlanePrefix.length();
                    newPath.clear().append(localPlane->queryPrefix());
                    if (strlen(dir))
                        newPath.append(dir); // add remaining tail of path
                    DBGLOG("Remapping logical file directory to '%s'", newPath.str());
                    file->setProp("@directory", newPath.str());
                }
            }
        }
        AccessMode accessMode = static_cast<AccessMode>(dfsFile->queryCommonMeta()->getPropInt("@accessMode"));
        fileDesc.setown(deserializeFileDescriptorTree(file));
        fileDesc->setTraceName(logicalName);
        // NB: the accessMode is being defined by the client call, and has been stored in the IDFSFile common meta
        // it is stored in turn into the IFileDescriptor properties for use by clients accessing paths
        fileDesc->queryProperties().setPropInt("@accessMode", static_cast<int>(accessMode));
        legacyDFSFile.setown(queryDistributedFileDirectory().createNew(fileDesc, logicalName));
    }
};

class CServiceSuperDistributedFile : public CServiceDistributedFileBase<IDistributedSuperFile>
{
    typedef CServiceDistributedFileBase<IDistributedSuperFile> PARENT;
    Owned<IDistributedSuperFile> legacyDFSSuperFile;

public:
    CServiceSuperDistributedFile(IDFSFile *_dfsFile) : PARENT(_dfsFile)
    {
        IArrayOf<IDistributedFile> subFiles;
        unsigned subs = dfsFile->numSubFiles();
        for (unsigned s=0; s<subs; s++)
        {
            Owned<IDFSFile> subFile = dfsFile->getSubFile(s);
            Owned<IDistributedFile> legacyDFSFile = createLegacyDFSFile(subFile);
            subFiles.append(*legacyDFSFile.getClear());
        }
        legacyDFSSuperFile.setown(queryDistributedFileDirectory().createNewSuperFile(dfsFile->queryFileMeta()->queryPropTree("SuperFile"), logicalName, &subFiles));
        legacyDFSFile.set(legacyDFSSuperFile);
        fileDesc.setown(legacyDFSSuperFile->getFileDescriptor());
    }
// IDistributedFile overrides
    virtual IDistributedSuperFile *querySuperFile() override
    {
        return this;
    }

// IDistributedSuperFile overrides
    virtual IDistributedFile &querySubFile(unsigned idx,bool sub) override
    {
        return legacyDFSSuperFile->querySubFile(idx, sub);
    }
    virtual IDistributedFile *querySubFileNamed(const char *name, bool sub) override
    {
        return legacyDFSSuperFile->querySubFileNamed(name, sub);
    }
    virtual IDistributedFile *getSubFile(unsigned idx,bool sub) override
    {
        return legacyDFSSuperFile->getSubFile(idx, sub);
    }
    virtual unsigned numSubFiles(bool sub) override
    {
        return legacyDFSSuperFile->numSubFiles(sub);
    }
    virtual bool isInterleaved() override
    {
        return legacyDFSSuperFile->isInterleaved();
    }
    virtual IDistributedFile *querySubPart(unsigned partidx,unsigned &subfileidx) override
    {
        return legacyDFSSuperFile->querySubPart(partidx, subfileidx);
    }
    virtual unsigned getPositionPart(offset_t pos, offset_t &base) override
    {
        return legacyDFSSuperFile->getPositionPart(pos, base);
    }
    virtual IDistributedFileIterator *getSubFileIterator(bool supersub=false) override
    {
        return legacyDFSSuperFile->getSubFileIterator(supersub);
    }
    virtual void validate() override
    {
        if (!legacyDFSSuperFile->existsPhysicalPartFiles(0))
        {
            const char * logicalName = queryLogicalName();
            throw makeStringExceptionV(-1, "Some physical parts do not exists, for logical file : %s",(isEmptyString(logicalName) ? "[unattached]" : logicalName));
        }
    }

// IDistributedSuperFile
    virtual void addSubFile(const char *subfile, bool before=false, const char *other=NULL, bool addcontents=false, IDistributedFileTransaction *transaction=NULL) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::addSubFile");
    }
    virtual bool removeSubFile(const char *subfile, bool remsub, bool remcontents=false, IDistributedFileTransaction *transaction=NULL) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::removeSubFile");
    }
    virtual bool removeOwnedSubFiles(bool remsub, IDistributedFileTransaction *transaction=NULL) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::removeOwnedSubFiles");
    }
    virtual bool swapSuperFile( IDistributedSuperFile *_file, IDistributedFileTransaction *transaction) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::swapSuperFile");
    }
};

static IDFSFile *createDFSFile(IPropertyTree *commonMeta, IPropertyTree *fileMeta, const char *remoteName, unsigned timeoutSecs, IUserDescriptor *userDesc);
class CDFSFile : public CSimpleInterfaceOf<IDFSFile>
{
    Linked<IPropertyTree> commonMeta; // e.g. share info between IFDSFiles, e.g. common plane info between subfiles
    Linked<IPropertyTree> fileMeta;
    unsigned __int64 lockId;
    std::vector<Owned<IDFSFile>> subFiles;
    StringAttr remoteName;
    unsigned timeoutSecs;
    Linked<IUserDescriptor> userDesc;

public:
    CDFSFile(IPropertyTree *_commonMeta, IPropertyTree *_fileMeta, const char *_remoteName, unsigned _timeoutSecs, IUserDescriptor *_userDesc)
        : commonMeta(_commonMeta), fileMeta(_fileMeta), remoteName(_remoteName), timeoutSecs(_timeoutSecs), userDesc(_userDesc)
    {
        lockId = fileMeta->getPropInt64("@lockId");
        if (fileMeta->getPropBool("@isSuper"))
        {
            Owned<IPropertyTreeIterator> iter = fileMeta->getElements("FileMeta");
            ForEach(*iter)
            {
                IPropertyTree &subMeta = iter->query();
                subFiles.push_back(createDFSFile(commonMeta, &subMeta, remoteName, timeoutSecs, userDesc));
            }
        }
    }
    virtual IPropertyTree *queryFileMeta() const override
    {
        return fileMeta;
    }
    virtual IPropertyTree *queryCommonMeta() const override
    {
        return commonMeta;
    }
    virtual unsigned __int64 getLockId() const override
    {
        return lockId;
    }
    virtual unsigned numSubFiles() const override
    {
        return (unsigned)subFiles.size();
    }
    virtual IDFSFile *getSubFile(unsigned idx) const override
    {
        return LINK(subFiles[idx]);
    }
    virtual const char *queryRemoteName() const override
    {
        return remoteName;
    }
    virtual IUserDescriptor *queryUserDescriptor() const override
    {
        return userDesc.getLink();
    }
    virtual unsigned queryTimeoutSecs() const override
    {
        return timeoutSecs;
    }
};

static IDFSFile *createDFSFile(IPropertyTree *commonMeta, IPropertyTree *fileMeta, const char *remoteName, unsigned timeoutSecs, IUserDescriptor *userDesc)
{
    return new CDFSFile(commonMeta, fileMeta, remoteName, timeoutSecs, userDesc);
}

IClientWsDfs *getDfsClient(const char *serviceUrl, IUserDescriptor *userDesc)
{
    // JCSMORE - can I reuse these, are they thread safe (AFishbeck?)

    VStringBuffer dfsUrl("%s/WsDfs", serviceUrl);
    Owned<IClientWsDfs> dfsClient = createWsDfsClient();
    dfsClient->addServiceUrl(dfsUrl);
    StringBuffer user, token;
    userDesc->getUserName(user),
    userDesc->getPassword(token);
    dfsClient->setUsernameToken(user, token, "");
    return dfsClient.getClear();
}

static void configureClientSSL(IEspClientRpcSettings &rpc, const char *secretName)
{
    Owned<const IPropertyTree> secretPTree = getSecret("storage", secretName);
    if (!secretPTree)
        throw makeStringExceptionV(-1, "secret %s.%s not found", "storage", secretName);

    StringBuffer certSecretBuf;
    if (!getSecretKeyValue(certSecretBuf, secretPTree, "tls.crt"))
        throw makeStringExceptionV(-1, "Client certificate 'tls.crt' missing from secret '%s'.", secretName);
    if (!containsEmbeddedKey(certSecretBuf))
        throw makeStringExceptionV(-1, "Client certificate content 'tls.crt' for secret '%s' not in expected format.", secretName);

    StringBuffer privKeySecretBuf;
    if (!getSecretKeyValue(privKeySecretBuf, secretPTree, "tls.key"))
        throw makeStringExceptionV(-1, "Client private key 'tls.crt' missing from secret '%s'.", secretName);
    if (!containsEmbeddedKey(privKeySecretBuf))
        throw makeStringExceptionV(-1, "Client private key content 'tls.key' for secret '%s' not in expected format.", secretName);

    StringBuffer caCertBuf;
    if (getSecretKeyValue(caCertBuf, secretPTree, "ca.crt"))
    {
        if (!containsEmbeddedKey(caCertBuf))
            throw makeStringExceptionV(-1, "CA certificate content 'ca.crt' for secret '%s' not in expected format.", secretName);
    }

    setRpcSSLOptionsBuf(rpc, true, certSecretBuf.str(), privKeySecretBuf.str(), caCertBuf.str(), false);
}

static CriticalSection serviceLeaseMapCS;
static std::unordered_map<std::string, unsigned __int64> serviceLeaseMap;
unsigned __int64 ensureClientLease(IClientWsDfs *dfsClient, const char *service, const char *secretName, IUserDescriptor *userDesc)
{
    CriticalBlock block(serviceLeaseMapCS);
    auto r = serviceLeaseMap.find(service);
    if (r != serviceLeaseMap.end())
        return r->second;

    Owned<IClientLeaseRequest> leaseReq = dfsClient->createGetLeaseRequest();
    if (!isEmptyString(secretName))
        configureClientSSL(leaseReq->rpc(), secretName);
    leaseReq->setKeepAliveExpiryFrequency(keepAliveExpiryFrequency);

    Owned<IClientLeaseResponse> leaseResp;

    unsigned timeoutSecs = 60;
    CTimeMon tm(timeoutSecs*1000);
    while (true)
    {
        try
        {
            leaseResp.setown(dfsClient->GetLease(leaseReq));

            unsigned __int64 leaseId = leaseResp->getLeaseId();
            serviceLeaseMap[service] = leaseId;
            return leaseId;
        }
        catch (IException *e)
        {
            /* NB: there should really be a different IException class and a specific error code
            * The server knows it's an unsupported method.
            */
            if (SOAP_SERVER_ERROR != e->errorCode())
                throw;
            e->Release();
        }

        if (tm.timedout())
            throw makeStringExceptionV(0, "GetLease timed out: timeoutSecs=%u", timeoutSecs);
        Sleep(5000); // sanity sleep
    }
}


#ifndef _CONTAINERIZED
static std::vector<std::string> dfsServiceUrls;
static CriticalSection dfsServiceUrlCrit;
static std::atomic<unsigned> currentDfsServiceUrl{0};
static bool dfsServiceUrlsDiscovered = false;
#endif

IDFSFile *lookupDFSFile(const char *logicalName, AccessMode accessMode, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc)
{
    CDfsLogicalFileName lfn;
    lfn.set(logicalName);
    StringBuffer remoteName, remoteLogicalFileName;
    StringBuffer serviceUrl;
    StringBuffer serviceSecret;
    bool secretProvided = false;
    bool useDafilesrv = false;
    if (lfn.isRemote())
    {
        verifyex(lfn.getRemoteSpec(remoteName, remoteLogicalFileName));

        // "local" is a reserved remote name, used to mean the local environment
        // which will be auto-discovered.
        if (!strieq(remoteName, "local"))
        {
            Owned<IPropertyTree> remoteStorage = getRemoteStorage(remoteName.str());
            if (!remoteStorage)
                throw makeStringExceptionV(0, "Remote storage '%s' not found", remoteName.str());
            serviceUrl.set(remoteStorage->queryProp("@service"));

            if (startsWith(serviceUrl, "https"))
            {
                // NB: standard configuration should not supply a secret, the secret name will be auto-generated based on the URL
                // If a manual secret name is defined, it will be used to connect to the DFS service and the dafilesrv services
                // A blank secret name can be defined to support connecting to bare-metal DFS services/dafilesrv's that do not support client certificates.
                if (remoteStorage->hasProp("@secret"))
                {
                    secretProvided = true;
                    serviceSecret.set(remoteStorage->queryProp("@secret"));
                }
            }

            logicalName = remoteLogicalFileName;
            useDafilesrv = remoteStorage->getPropBool("@useDafilesrv");
        }
    }
    if (!serviceUrl.length())
    {
        // auto-discover local environment dfs service.
#ifdef _CONTAINERIZED
        // NB: only expected to be here if experimental option #option('dfsesp-localfiles', true); is in use.
        // This finds and uses local dfs service for local read lookups.
        Owned<IPropertyTreeIterator> eclWatchServices = getGlobalConfigSP()->getElements("services[@type='dfs']");
        if (!eclWatchServices->first())
            throw makeStringException(-1, "Dfs service not defined in esp services");
        const IPropertyTree &eclWatch = eclWatchServices->query();
        StringBuffer eclWatchName;
        eclWatch.getProp("@name", eclWatchName);
        const char *protocol = eclWatch.getPropBool("@tls") ? "https" : "http";
        unsigned port = (unsigned)eclWatch.getPropInt("@port", NotFound);
        if (NotFound == port)
            throw makeStringExceptionV(-1, "dfs '%s': service port not defined", eclWatchName.str());
        serviceUrl.appendf("%s://%s:%u", protocol, eclWatchName.str(), port);
#else
        {
            CriticalBlock b(dfsServiceUrlCrit);
            if (!dfsServiceUrlsDiscovered)
            {
                dfsServiceUrlsDiscovered = true;
                getAccessibleServiceURLList("WsSMC", dfsServiceUrls);
                if (0 == dfsServiceUrls.size())
                    throw makeStringException(-1, "Could not find any DFS services in the target HPCC configuration.");
            }
        }
        serviceUrl.append(dfsServiceUrls[currentDfsServiceUrl].c_str());
        currentDfsServiceUrl = (currentDfsServiceUrl+1 == dfsServiceUrls.size()) ? 0 : currentDfsServiceUrl+1;
        remoteName.clear(); // local
#endif
    }
    bool useSSL = startsWith(serviceUrl, "https");
    if (useSSL && !secretProvided)
        generateDynamicUrlSecretName(serviceSecret, serviceUrl, nullptr);

    DBGLOG("Looking up file '%s' on '%s'", logicalName, serviceUrl.str());
    Owned<IClientWsDfs> dfsClient = getDfsClient(serviceUrl, userDesc);

    unsigned __int64 clientLeaseId = ensureClientLease(dfsClient, serviceUrl, serviceSecret, userDesc);

    Owned<IClientDFSFileLookupResponse> dfsResp;
    Owned<IClientDFSFileLookupRequest> dfsReq = dfsClient->createDFSFileLookupRequest();
    if (useSSL && serviceSecret.length())
        configureClientSSL(dfsReq->rpc(), serviceSecret.str());
    dfsReq->setAccessViaDafilesrv(useDafilesrv);
    // JCSMORE may want to pass accessMode to server, for it to decide and pre-filter the aliases/
    // For now, set into IDFSFile created locally (see below)
    dfsReq->setName(logicalName);
    dfsReq->setLeaseId(clientLeaseId);
    CTimeMon tm(timeoutSecs*1000); // NB: this timeout loop is to cater for *a* esp disappearing (e.g. if behind load balancer)
    while (true)
    {
        try
        {
            unsigned remaining;
            if (tm.timedout(&remaining))
                break;
            dfsReq->setRequestTimeout(remaining/1000);
            dfsResp.setown(dfsClient->DFSFileLookup(dfsReq));

            const IMultiException *excep = &dfsResp->getExceptions(); // NB: warning despite getXX name, this does not Link
            if (excep->ordinality() > 0)
                throw LINK((IMultiException *)excep); // NB - const IException.. not caught in general..

            const char *base64Resp = dfsResp->getMeta();
            MemoryBuffer compressedRespMb;
            JBASE64_Decode(base64Resp, compressedRespMb);
            MemoryBuffer decompressedRespMb;
            fastLZDecompressToBuffer(decompressedRespMb, compressedRespMb);
            Owned<IPropertyTree> meta = createPTree(decompressedRespMb);
            IPropertyTree *fileMeta = meta->queryPropTree("FileMeta");
            if (!fileMeta) // file not found
                return nullptr;
            meta->setPropInt("@accessMode", static_cast<unsigned>(accessMode));
            // remoteName empty if local
            return createDFSFile(meta, fileMeta, remoteName.length()?remoteName.str():nullptr, timeoutSecs, userDesc);
        }
        catch (IException *e)
        {
            /* NB: there should really be a different IException class and a specific error code
            * The server knows it's an unsupported method.
            */
            if (SOAP_SERVER_ERROR != e->errorCode())
                throw;
            e->Release();
        }

        if (tm.timedout())
            break;
        Sleep(5000); // sanity sleep
    }
    throw makeStringExceptionV(0, "DFSFileLookup timed out: file=%s, timeoutSecs=%u", logicalName, timeoutSecs);
}

IDistributedFile *createLegacyDFSFile(IDFSFile *dfsFile)
{
    if (dfsFile->queryFileMeta()->getPropBool("@isSuper"))
        return new CServiceSuperDistributedFile(dfsFile);
    else
        return new CServiceDistributedFile(dfsFile);
}

IDistributedFile *lookupLegacyDFSFile(const char *logicalName, AccessMode accessMode, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc)
{
    Owned<IDFSFile> dfsFile = lookupDFSFile(logicalName, accessMode, timeoutSecs, keepAliveExpiryFrequency, userDesc);
    if (!dfsFile)
        return nullptr;
    return createLegacyDFSFile(dfsFile);
}

IDistributedFile *lookup(CDfsLogicalFileName &lfn, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, bool priviledged, unsigned timeout)
{
    bool viaDali = false;

    bool isForeign = false;
    try { isForeign = lfn.isForeign(); }
    catch(IException *e) { e->Release(); } // catch and ignore multi lfn case, will be checked later
    if (isForeign && !allowForeign())
        throw makeStringExceptionV(0, "foreign access is not permitted from this system (file='%s')", lfn.get());

    // DFS service currently only supports remote files 
    if (isWrite(accessMode))
        viaDali = true;
    else
    {
        // switch to Dali if non-remote file, unless "dfsesp-localfiles" enabled (and non-external) 
        if (!lfn.isRemote())
        {
            if (lfn.isExternal() || (!getComponentConfigSP()->getPropBool("dfsesp-localfiles")))
                viaDali = true;
        }
    }
    if (viaDali)
        return queryDistributedFileDirectory().lookup(lfn, user, accessMode, hold, lockSuperOwner, transaction, priviledged, timeout);

    return wsdfs::lookupLegacyDFSFile(lfn.get(), accessMode, timeout, wsdfs::keepAliveExpiryFrequency, user);
}

IDistributedFile *lookup(const char *logicalFilename, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, bool priviledged, unsigned timeout)
{
    CDfsLogicalFileName lfn;
    lfn.set(logicalFilename);
    return lookup(lfn, user, accessMode, hold, lockSuperOwner, transaction, priviledged, timeout);
}

bool exists(CDfsLogicalFileName &lfn, IUserDescriptor *user, bool notSuper, bool superOnly, unsigned timeout)
{
    if (!lfn.isRemote())
        return queryDistributedFileDirectory().exists(lfn.get(), user, notSuper, superOnly);

    Owned<IDistributedFile> file = lookup(lfn, user, AccessMode::read, false, false, nullptr, false, timeout);
    if (!file)
        return false;
    bool isSuper = nullptr != file->querySuperFile();
    if (superOnly)
    {
        if (!isSuper)
            return false;
    }
    else if (notSuper)
    {
        if (isSuper)
            return false;
    }
    return true;
}

bool exists(const char *logicalFilename, IUserDescriptor *user, bool notSuper, bool superOnly, unsigned timeout)
{
    CDfsLogicalFileName lfn;
    lfn.set(logicalFilename);
    return exists(lfn, user, notSuper, superOnly, timeout);
}


} // namespace wsdfs


class CLocalOrDistributedFile: implements ILocalOrDistributedFile, public CInterface
{
    bool fileExists;
    Owned<IDistributedFile> dfile;
    CDfsLogicalFileName lfn;    // set if localpath but prob not useful
    StringAttr localpath;
    StringAttr fileDescPath;
public:
    IMPLEMENT_IINTERFACE;
    CLocalOrDistributedFile()
    {
        fileExists = false;
    }

    virtual const char *queryLogicalName() override
    {
        return lfn.get();
    }

    virtual IDistributedFile * queryDistributedFile() override
    { 
        return dfile.get(); 
    }

    bool init(const char *fname,IUserDescriptor *user,bool onlylocal,bool onlydfs, AccessMode accessMode, bool isPrivilegedUser, const StringArray *clusters)
    {
        fileExists = false;
        if (!onlydfs)
            lfn.allowOsPath(true);
        if (!lfn.setValidate(fname))
            return false;
        bool write = isWrite(accessMode);
        if (!onlydfs)
        {
            bool gotlocal = true;
            if (isAbsolutePath(fname)||(stdIoHandle(fname)>=0)) 
                localpath.set(fname);
            else if (!strstr(fname,"::"))
            {
                // treat it as a relative file
                StringBuffer fn;
                localpath.set(makeAbsolutePath(fname,fn).str());
            }
            else if (!lfn.isExternal())
                gotlocal = false;
            if (gotlocal)
            {
                if (!write && !onlylocal) // MORE - this means the dali access checks not happening... maybe that's ok?
                    dfile.setown(wsdfs::lookup(lfn, user, accessMode, false, false, nullptr, isPrivilegedUser, INFINITE));
                Owned<IFile> file = getPartFile(0,0);
                if (file.get())
                {
                    fileExists = file->exists();
                    return fileExists || write;
                }
            }
        }
        if (!onlylocal)
        {
            if (lfn.isExternalFile() || lfn.isExternalPlane())
            {
                Owned<IFileDescriptor> fDesc = createExternalFileDescriptor(lfn.get());
                dfile.setown(queryDistributedFileDirectory().createExternal(fDesc, lfn.get()));
                Owned<IFile> file = getPartFile(0,0);
                if (file.get())
                    fileExists = file->exists();
                if (write && lfn.isExternal()&&(dfile->numParts()==1))   // if it is writing to an external file then don't return distributed
                    dfile.clear();
                return true;
            }
            else
            {
                dfile.setown(wsdfs::lookup(lfn, user, accessMode, false, false, nullptr, isPrivilegedUser, INFINITE));
                if (dfile.get())
                    return true;
            }

            StringBuffer dir;
            unsigned stripeNum = 0;
#ifdef _CONTAINERIZED
            StringBuffer cluster;
            if (clusters)
            {
                if (clusters->ordinality()>1)
                    throw makeStringExceptionV(0, "Container mode does not yet support output to multiple clusters while writing file %s)", fname);
                cluster.append(clusters->item(0));
            }
            else
                getDefaultStoragePlane(cluster);
            Owned<IStoragePlane> plane = getDataStoragePlane(cluster, true);
            dir.append(plane->queryPrefix());
            unsigned numStripedDevices = plane->numDevices();
            stripeNum = calcStripeNumber(0, lfn.get(), numStripedDevices);
#endif
            StringBuffer descPath;
            makePhysicalDirectory(descPath, lfn.get(), 0, DFD_OSdefault, dir);
            fileDescPath.set(descPath);

            // MORE - should we create the IDistributedFile here ready for publishing (and/or to make sure it's locked while we write)?
            StringBuffer physicalPath;
            makePhysicalPartName(lfn.get(), 1, 1, physicalPath, 0, DFD_OSdefault, dir, false, stripeNum); // more - may need to override path for roxie
            localpath.set(physicalPath);
            fileExists = (dfile != NULL);
            return write;
        }
        return false;
    }

    virtual IFileDescriptor *getFileDescriptor() override
    {
        if (dfile.get())
            return dfile->getFileDescriptor();
        Owned<IFileDescriptor> fileDesc = createFileDescriptor();
        fileDesc->setTraceName(lfn.get());
        StringBuffer dir;
        if (localpath.isEmpty()) { // e.g. external file
            StringBuffer tail;
            IException *e=NULL;
            bool iswin=
#ifdef _WIN32
                true;
#else
                false;
#endif
            if (!lfn.getExternalPath(dir,tail,iswin,&e)) {
                if (e)
                    throw e;
                return NULL;
            }
        }
        else 
            splitDirTail(fileDescPath,dir);
        fileDesc->setDefaultDir(dir.str());
        RemoteFilename rfn;
        getPartFilename(rfn,0,0);
        fileDesc->setPart(0,rfn);
        fileDesc->queryPartDiskMapping(0).defaultCopies = DFD_DefaultCopies;
        return fileDesc.getClear();
    }

    virtual bool getModificationTime(CDateTime &dt) override
    {
        if (dfile.get())
            return dfile->getModificationTime(dt);
        Owned<IFile> file = getPartFile(0,0);
        if (file.get()) {
            CDateTime dt;
            return file->getTime(NULL,&dt,NULL);
        }
        return false;
    }

    virtual unsigned numParts() override 
    {
        if (dfile.get()) 
            return dfile->numParts();
        return 1;
    }

    virtual unsigned numPartCopies(unsigned partnum) override
    {
        if (dfile.get()) 
            return dfile->queryPart(partnum).numCopies();
        return 1;
    }
    
    virtual IFile *getPartFile(unsigned partnum,unsigned copy) override
    {
        RemoteFilename rfn;
        if ((partnum==0)&&(copy==0))
            return createIFile(getPartFilename(rfn,partnum,copy));
        return NULL;
    }
    
    virtual void getDirAndFilename(StringBuffer &dir, StringBuffer &filename) override
    {
        if (dfile.get())
        {
            dir.append(dfile->queryDefaultDir());
            splitFilename(localpath, nullptr, nullptr, &filename, &filename);
        }
        else if (localpath.isEmpty())
        {
            RemoteFilename rfn;
            lfn.getExternalFilename(rfn);
            StringBuffer fullPath;
            rfn.getLocalPath(fullPath);
            splitFilename(localpath, nullptr, &dir, &filename, &filename);
        }
        else
        {
            dir.append(fileDescPath);
            splitFilename(localpath, nullptr, nullptr, &filename, &filename);
        }
    }

    virtual RemoteFilename &getPartFilename(RemoteFilename &rfn, unsigned partnum,unsigned copy) override
    {
        if (dfile.get()) 
            dfile->queryPart(partnum).getFilename(rfn,copy);
        else if (localpath.isEmpty())
            lfn.getExternalFilename(rfn);
        else
            rfn.setRemotePath(localpath);
        return rfn;
    }

    StringBuffer &getPartFilename(StringBuffer &path, unsigned partnum,unsigned copy)
    {
        RemoteFilename rfn;
        if (dfile.get()) 
            dfile->queryPart(partnum).getFilename(rfn,copy);
        else if (localpath.isEmpty())
            lfn.getExternalFilename(rfn);
        else 
            path.append(localpath);
        if (rfn.isLocal())
            rfn.getLocalPath(path);
        else
            rfn.getRemotePath(path);
        return path;
    }

    virtual bool getPartCrc(unsigned partnum, unsigned &crc) override
    {
        if (dfile.get())  
            return dfile->queryPart(partnum).getCrc(crc);
        Owned<IFile> file = getPartFile(0,0);
        if (file.get()) {
            crc = file->getCRC();
            return true;
        }
        return false;
    }

    virtual offset_t getPartFileSize(unsigned partnum) override
    {
        if (dfile.get()) 
            return dfile->queryPart(partnum).getFileSize(true,false);
        Owned<IFile> file = getPartFile(0,0);
        if (file.get())
            return file->size();
        return (offset_t)-1;
    }

    virtual offset_t getFileSize() override
    {
        if (dfile.get())
            dfile->getFileSize(true,false);
        offset_t ret = 0;
        unsigned np = numParts();
        for (unsigned i = 0;i<np;i++)
            ret += getPartFileSize(i);
        return ret;
    }

    virtual bool exists() const override
    {
        return fileExists;
    }

    virtual bool isExternal() const override
    {
        return lfn.isExternal();
    }

    virtual bool isExternalFile() const override
    {
        return lfn.isExternalFile() || lfn.isExternalPlane();
    }
};


ILocalOrDistributedFile* createLocalOrDistributedFile(const char *fname,IUserDescriptor *user,bool onlylocal,bool onlydfs, AccessMode accessMode, bool isPrivilegedUser, const StringArray *clusters)
{
    Owned<CLocalOrDistributedFile> ret = new CLocalOrDistributedFile();
    if (ret->init(fname,user,onlylocal,onlydfs,accessMode,isPrivilegedUser,clusters))
        return ret.getClear();
    return NULL;
}


