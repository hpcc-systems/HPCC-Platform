/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#define da_decl __declspec(dllexport)
#include "platform.h"
#include "portlist.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jiter.ipp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jlzw.hpp"
#include "dafdesc.hpp"
#include "rmtfile.hpp"
#include "dautils.hpp"
#include "dasds.hpp"

#include "dafdesc.hpp"
#include "dadfs.hpp"

#define INCLUDE_1_OF_1    // whether to use 1_of_1 for single part files

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite

#define SERIALIZATION_VERSION ((byte)0xd4)
#define SERIALIZATION_VERSION2 ((byte)0xd5) // with trailing superfile info

bool isMulti(const char *str)
{
    if (str&&!isSpecialPath(str))
        loop {
            switch (*str) {
            case ',':
            case '*':
            case '?':
                return true;
            case 0:
                return false;
            }
            str++;
        }
    return false;
}


bool isCompressed(IPropertyTree &props, bool *blocked)
{
    if (props.getPropBool("@blockCompressed"))
    {
        if (blocked) *blocked = true;
        return true;
    }
    else
    {
        if (blocked) *blocked = false;
        return props.getPropBool("@rowCompressed");
    }
}

bool getCrcFromPartProps(IPropertyTree &fileattr,IPropertyTree &props, unsigned &crc)
{
    if (props.hasProp("@fileCrc"))
    {
        crc = (unsigned)props.getPropInt64("@fileCrc");
        return true;
    }
    // NB: old @crc keys and compressed were not crc of file but of data within.
    const char *kind = props.queryProp("@kind");
    if (kind&&strcmp(kind,"key")) // key part
        return false;
    bool blocked;
    if (isCompressed(fileattr,&blocked)) {
        if (!blocked)
            return false;
        crc = COMPRESSEDFILECRC;
        return true;
    }
    if (!props.hasProp("@crc"))
        return false;
    crc = (unsigned)props.getPropInt64("@crc");
    return true;
}

void ClusterPartDiskMapSpec::setRoxie (unsigned redundancy, unsigned channelsPerNode, int _replicateOffset)
{
    flags = 0;
    replicateOffset = _replicateOffset?_replicateOffset:1;
    defaultCopies = redundancy+1;
    if ((channelsPerNode>1)&&(redundancy==0)) {
        flags |= CPDMSF_wrapToNextDrv;
        flags |= CPDMSF_overloadedConfig;
        maxDrvs = channelsPerNode;
    }
    else
        maxDrvs = (redundancy>1)?(redundancy+1):2;
    if (_replicateOffset==0)
        flags |= CPDMSF_fillWidth;
    startDrv = 0;
}

bool ClusterPartDiskMapSpec::calcPartLocation (unsigned part, unsigned maxparts, unsigned copy, unsigned clusterwidth, unsigned &node, unsigned &drv)
{
    // this is more cryptic than it could be (e.g. by special casing)
    // because it handles the cases that aren't going to ever happen, in a general way
    node = 0;
    drv = 0;
    if (!clusterwidth||!maxparts)
        return false;
    if (part>=maxparts)
        return false;

    unsigned nc = numCopies(part,clusterwidth,maxparts);
    if (copy>=nc)
        return false;
    unsigned dc=defaultCopies?defaultCopies:DFD_DefaultCopies;
    drv = startDrv;
    bool fw = (flags&CPDMSF_fillWidth)!=0;
    if (fw&&(maxparts>clusterwidth/2))
        fw = false;
    // calc primary
    node = part%clusterwidth;
    unsigned repdrv = startDrv+1;
    if (flags&CPDMSF_wrapToNextDrv) {
        drv += startDrv+(part/clusterwidth)%maxDrvs;
        repdrv = (1+(maxparts-1)/clusterwidth)%maxDrvs;
    }
    if (copy) {
        if (fw) {
            if (interleave>1)
                ERRLOG("ClusterPartDiskMapSpec interleave not allowed if fill width set");
            if (flags&CPDMSF_repeatedPart)
                ERRLOG("ClusterPartDiskMapSpec repeated part not allowed if fill width set");
            unsigned m = clusterwidth/maxparts;
            drv = startDrv+(repdrv+(copy/m-1))%maxDrvs;
            node += (copy%m)*maxparts;
        }
        else if ((flags&CPDMSF_repeatedPart)) {
            if (flags&CPDMSF_wrapToNextDrv)
                ERRLOG("ClusterPartDiskMapSpec repeated part not allowed if wrap to next drive set");
            unsigned repnum = copy%dc;
            unsigned nodenum = copy/dc;
            drv = startDrv+repnum%maxDrvs;
            if (interleave>1)
                node = (node+nodenum+(replicateOffset*repnum*interleave))%clusterwidth;
            else
                node = (node+nodenum+(replicateOffset*repnum))%clusterwidth;
        }
        else {
            drv = startDrv+(repdrv+copy-1)%maxDrvs;
            if (interleave>1)
                node = (node+(replicateOffset*copy*interleave))%clusterwidth;
            else
                node = (node+(replicateOffset*copy))%clusterwidth;
        }
    }
    return true;
}


inline void setPropDef(IPropertyTree *tree,const char *prop,int val,int def)
{
    if (val!=def)
        tree->setPropInt(prop,val);
    else
        tree->removeProp(prop);
}

inline int getPropDef(IPropertyTree *tree,const char *prop,int def)
{
    if (tree)
        return tree->getPropInt(prop,def);
    return def;
}

void ClusterPartDiskMapSpec::toProp(IPropertyTree *tree)
{
    if (!tree)
        return;
    setPropDef(tree,"@replicateOffset",replicateOffset,1);
    setPropDef(tree,"@redundancy",defaultCopies?(defaultCopies-1):1,1);
    setPropDef(tree,"@maxDrvs",maxDrvs?maxDrvs:2,2);
    setPropDef(tree,"@startDrv",startDrv,0);
    setPropDef(tree,"@interleave",interleave,0);
    setPropDef(tree,"@mapFlags",flags,0);
    setPropDef(tree,"@repeatedPart",repeatedPart,(int)CPDMSRP_notRepeated);
    if (defaultBaseDir.isEmpty())
        tree->removeProp("@defaultBaseDir");
    else
        tree->setProp("@defaultBaseDir",defaultBaseDir);
    if (defaultReplicateDir.isEmpty())
        tree->removeProp("@defaultReplicateDir");
    else
        tree->setProp("@defaultReplicateDir",defaultReplicateDir);
}

void ClusterPartDiskMapSpec::fromProp(IPropertyTree *tree)
{
    unsigned defrep = 1;
    // if directory is specified then must match default base to be default replicated
    StringBuffer dir;
    if (tree&&tree->getProp("@directory",dir)) {
        const char * base = queryBaseDirectory(grp_unknown, 0, SepCharBaseOs(getPathSepChar(dir.str())));
        size32_t l = strlen(base);
        if ((memcmp(base,dir.str(),l)!=0)||((l!=dir.length())&&!isPathSepChar(dir.charAt(l))))
            defrep = 0;
    }
    replicateOffset = getPropDef(tree,"@replicateOffset",1);
    defaultCopies = getPropDef(tree,"@redundancy",defrep)+1;
    maxDrvs = (byte)getPropDef(tree,"@maxDrvs",2);
    startDrv = (byte)getPropDef(tree,"@startDrv",defrep?0:getPathDrive(dir.str()));
    interleave = getPropDef(tree,"@interleave",0);
    flags = (byte)getPropDef(tree,"@mapFlags",0);
    repeatedPart = (unsigned)getPropDef(tree,"@repeatedPart",(int)CPDMSRP_notRepeated);
    setDefaultBaseDir(tree->queryProp("@defaultBaseDir"));
    setDefaultReplicateDir(tree->queryProp("@defaultReplicateDir"));
}

void ClusterPartDiskMapSpec::serialize(MemoryBuffer &mb)
{
    mb.append(flags);
    mb.append(replicateOffset);
    mb.append(defaultCopies);
    mb.append(startDrv);
    mb.append(maxDrvs);
    mb.append(interleave);
    if (flags&CPDMSF_repeatedPart)
        mb.append(repeatedPart);
    if (flags&CPDMSF_defaultBaseDir)
        mb.append(defaultBaseDir);
    if (flags&CPDMSF_defaultReplicateDir)
        mb.append(defaultReplicateDir);
}

void ClusterPartDiskMapSpec::deserialize(MemoryBuffer &mb)
{
    mb.read(flags);
    mb.read(replicateOffset);
    mb.read(defaultCopies);
    mb.read(startDrv);
    mb.read(maxDrvs);
    mb.read(interleave);
    if (flags&CPDMSF_repeatedPart)
        mb.read(repeatedPart);
    else
        repeatedPart = CPDMSRP_notRepeated;
    if (flags&CPDMSF_defaultBaseDir)
        mb.read(defaultBaseDir);
    else
        defaultBaseDir.clear();
    if (flags&CPDMSF_defaultReplicateDir)
        mb.read(defaultReplicateDir);
    else
        defaultReplicateDir.clear();
}


void ClusterPartDiskMapSpec::ensureReplicate()
{
    if (defaultCopies <= DFD_NoCopies)
        defaultCopies = DFD_DefaultCopies;
}

bool ClusterPartDiskMapSpec::isReplicated() const
{
    // If defaultCopies is zero (deprecated/legacy), the default value for replicated is true
    // Else, if it has any copy (>= 2), than it is replicated
    return defaultCopies != DFD_NoCopies;
}

unsigned ClusterPartDiskMapSpec::numCopies(unsigned part,unsigned clusterwidth, unsigned filewidth)
{
    if (flags&CPDMSF_repeatedPart) {
        if (repeatedPart&CPDMSRP_lastRepeated) {
            if (part+1==filewidth)
                return clusterwidth*defaultCopies;
        }
        else if ((part==(repeatedPart&CPDMSRP_partMask))||(repeatedPart&CPDMSRP_allRepeated))
            return clusterwidth*defaultCopies;
        if (repeatedPart&CPDMSRP_onlyRepeated)
            return 0;
    }
    return defaultCopies;
}

void ClusterPartDiskMapSpec::setRepeatedCopies(unsigned partnum,bool onlyrepeats)
{
    repeatedPart = partnum;
    if (partnum!=CPDMSRP_notRepeated) {
        flags |= CPDMSF_repeatedPart;
        if (onlyrepeats)
            repeatedPart |= CPDMSRP_onlyRepeated;
    }
    else
        flags &= ~CPDMSF_repeatedPart;
}

void ClusterPartDiskMapSpec::setDefaultBaseDir(const char *dir)
{
    defaultBaseDir.set(dir);
    if (defaultBaseDir.isEmpty())
        flags &= ~CPDMSF_defaultBaseDir;
    else
        flags |= CPDMSF_defaultBaseDir;
}

void ClusterPartDiskMapSpec::setDefaultReplicateDir(const char *dir)
{
    defaultReplicateDir.set(dir);
    if (defaultReplicateDir.isEmpty())
        flags &= ~CPDMSF_defaultReplicateDir;
    else
        flags |= CPDMSF_defaultReplicateDir;
}


ClusterPartDiskMapSpec & ClusterPartDiskMapSpec::operator=(const ClusterPartDiskMapSpec &other)
{
    replicateOffset = other.replicateOffset;
    defaultCopies = other.defaultCopies;
    maxDrvs = other.maxDrvs;
    startDrv = other.startDrv;
    flags = other.flags;
    interleave = other.interleave;
    repeatedPart = other.repeatedPart;
    setDefaultBaseDir(other.defaultBaseDir);
    setDefaultReplicateDir(other.defaultReplicateDir);
    return *this;
}


// --------------------------------------------------------



static void removeDir(const char *name,const char *dir,StringBuffer &out)
{
    const char *s=name;
    const char *d=dir;
    if (d&&*d) {
        while (*s&&(toupper(*s)==toupper(*d))) {
            s++;
            d++;
        }
        if ((*d==0)&&isPathSepChar(*s))  // support cross OS
            name = s+1;
    }
    out.append(name);
}

#define RO_SINGLE_PART (0x40000000) // used for singletons

struct CClusterInfo: public CInterface, implements IClusterInfo
{
    Linked<IGroup> group;
    StringAttr name; // group name
    ClusterPartDiskMapSpec mspec;
    void checkClusterName(INamedGroupStore *resolver)
    {
        // check name matches group
        if (resolver&&group) {
            if (!name.isEmpty()) {
                StringBuffer defaultDir;
                GroupType groupType;
                Owned<IGroup> lgrp = resolver->lookup(name, defaultDir, groupType);
                if (lgrp&&lgrp->equals(group))
                {
                    if (mspec.defaultBaseDir.isEmpty())
                    {
                        mspec.setDefaultBaseDir(defaultDir);   // MORE - should possibly set up the rest of the mspec info from the group info here
                    }
                    if (mspec.defaultCopies>1 && mspec.defaultReplicateDir.isEmpty())
                    {
                        mspec.setDefaultReplicateDir(queryBaseDirectory(groupType, 1));  // MORE - not sure this is strictly correct
                    }
                    return; // ok
                }
                name.clear();
            }
            StringBuffer gname;
            if (resolver->find(group,gname,true)||(group->ordinality()>1))
                name.set(gname);
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    CClusterInfo(MemoryBuffer &mb,INamedGroupStore *resolver)
    {
        StringAttr grptext;
        mb.read(grptext);
        if (!grptext.isEmpty())
            group.setown(createIGroup(grptext));
        mspec.deserialize(mb);
        mb.read(name);
        checkClusterName(resolver);
    }

    CClusterInfo(const char *_name,IGroup *_group,const ClusterPartDiskMapSpec &_mspec,INamedGroupStore *resolver)
        : name(_name),group(_group)
    {
        name.toLowerCase();
        mspec =_mspec;
        checkClusterName(resolver);
    }
    CClusterInfo(IPropertyTree *pt,INamedGroupStore *resolver,unsigned flags)
    {
        if (!pt)
            return;
        name.set(pt->queryProp("@name"));
        mspec.fromProp(pt);
        if ((((flags&IFDSF_EXCLUDE_GROUPS)==0)||name.isEmpty())&&pt->hasProp("Group"))
            group.setown(createIGroup(pt->queryProp("Group")));
        if (!name.isEmpty()&&!group.get()&&resolver)
        {
            StringBuffer defaultDir;
            GroupType groupType;
            group.setown(resolver->lookup(name.get(), defaultDir, groupType));
            // MORE - common some of this with checkClusterName?
            if (mspec.defaultBaseDir.isEmpty())
            {
                mspec.setDefaultBaseDir(defaultDir);   // MORE - should possibly set up the rest of the mspec info from the group info here
            }
            if (mspec.defaultCopies>1 && mspec.defaultReplicateDir.isEmpty())
            {
                mspec.setDefaultReplicateDir(queryBaseDirectory(groupType, 1));  // MORE - not sure this is strictly correct
            }
        }
        else
            checkClusterName(resolver);
    }

    const char *queryGroupName()
    {
        return name.isEmpty()?NULL:name.get();
    }

    IGroup *queryGroup(IGroupResolver *resolver)
    {
        if (!group&&!name.isEmpty()&&resolver)
            group.setown(resolver->lookup(name));
        return group.get();
    }

    StringBuffer &getGroupName(StringBuffer &ret,IGroupResolver *resolver)
    {
        if (name.isEmpty()) {
            if (group)
            {
               if (resolver)
                    resolver->find(group,ret,true); // this will set single node as well
               else if (group->ordinality()==1)
                    group->getText(ret);
            }
        }
        else
            ret.append(name);
        return ret;
    }

    void serialize(MemoryBuffer &mb)
    {
        StringBuffer grptext;
        if (group)
            group->getText(grptext);
        mb.append(grptext);
        mspec.serialize(mb);
        mb.append(name);
    }
    INode *queryNode(unsigned idx,unsigned maxparts,unsigned copy)
    {
        if (!group.get())
            return queryNullNode();
        unsigned nn;
        unsigned dn;
        if (!mspec.calcPartLocation (idx,maxparts,copy, group->ordinality(), nn, dn))
            return queryNullNode();
        return &group->queryNode(nn);
    }
    unsigned queryDrive(unsigned idx,unsigned maxparts,unsigned copy)
    {
        if (!group.get())
            return 0;
        unsigned nn;
        unsigned dn;
        mspec.calcPartLocation (idx,maxparts,copy, group->ordinality(), nn, dn);
        return dn;
    }

    void serializeTree(IPropertyTree *pt,unsigned flags)
    {
        mspec.toProp(pt);
        if (group&&(((flags&IFDSF_EXCLUDE_GROUPS)==0)||name.isEmpty())) {
            StringBuffer gs;
            group->getText(gs);
            pt->setProp("Group",gs.str());
        }
        if (!name.isEmpty()&&((flags&IFDSF_EXCLUDE_CLUSTERNAMES)==0))
            pt->setProp("@name",name);
    }

    ClusterPartDiskMapSpec  &queryPartDiskMapping()
    {
        return mspec;
    }

    void setGroupName(const char *_name)
    {
        name.set(_name);
        name.toLowerCase();
    }

    void setGroup(IGroup *_group)
    {
        group.set(_group);
    }

    IGroup *queryGroup()
    {
        return group;
    }

    void getBaseDir(StringBuffer &basedir,DFD_OS os)
    {
        if (mspec.defaultBaseDir.isEmpty())  // assume current platform's default
            basedir.append(queryBaseDirectory(grp_unknown, 0, os));
        else
            basedir.append(mspec.defaultBaseDir);
    }

    void getReplicateDir(StringBuffer &basedir,DFD_OS os)
    {
        if (mspec.defaultReplicateDir.isEmpty())  // assume current platform's default
            basedir.append(queryBaseDirectory(grp_unknown, 1, os));
        else
            basedir.append(mspec.defaultReplicateDir);
    }

    StringBuffer &getClusterLabel(StringBuffer &ret)
    {
        return getGroupName(ret, NULL);
    }

};

IClusterInfo *createClusterInfo(const char *name,
                                IGroup *grp,
                                const ClusterPartDiskMapSpec &mspec,
                                INamedGroupStore *resolver)
{
    return new CClusterInfo(name,grp,mspec,resolver);
}
IClusterInfo *deserializeClusterInfo(MemoryBuffer &mb,
                                INamedGroupStore *resolver)
{
    return new CClusterInfo(mb,resolver);
}

IClusterInfo *deserializeClusterInfo(IPropertyTree *pt,
                                INamedGroupStore *resolver,
                                unsigned flags)
{
    return new CClusterInfo(pt,resolver,flags);
}


class CFileDescriptorBase: public CInterface
{
protected:
    PointerArray parts; // of CPartDescriptor

public:

    StringAttr tracename;
    IArrayOf<IClusterInfo> clusters;

    Owned<IPropertyTree> attr;
    StringAttr directory;
    StringAttr partmask;
    virtual unsigned numParts() = 0;                                            // number of parts
    virtual unsigned numCopies(unsigned partnum) = 0;                           // number of copies
    virtual INode *doQueryNode(unsigned partidx, unsigned copy, unsigned rn) = 0;               // query machine node
    virtual unsigned queryDrive(unsigned partidx, unsigned copy) = 0;           // query drive
    virtual StringBuffer &getPartTail(StringBuffer &name,unsigned idx) = 0;
    virtual StringBuffer &getPartDirectory(StringBuffer &name,unsigned idx,unsigned copy = 0) = 0;  // get filename dir
    virtual void serializePart(MemoryBuffer &mb,unsigned idx) = 0;
    virtual const char *queryDefaultDir() = 0;
    virtual IFileDescriptor &querySelf() = 0;
    virtual unsigned copyClusterNum(unsigned partidx, unsigned copy,unsigned *replicate=NULL) = 0;

};

class CPartDescriptor : implements IPartDescriptor
{
protected: friend class CFileDescriptor;

    StringAttr overridename;    // this may be a multi path - may or not be relative to directory
                        // if not set use parent mask (and is *not* multi in this case)
    bool ismulti;       // only set if overridename set (otherwise false)
    CFileDescriptorBase &parent; // this is for the cluster *not* for the entire file

    unsigned partIndex;
    Owned<IPropertyTree> props;

public:

    virtual void Link(void) const
    {
        parent.Link();
    }
    virtual bool Release(void) const
    {
        return parent.Release();
    }


    CPartDescriptor(CFileDescriptorBase &_parent,unsigned idx,IPropertyTree *pt)
        : parent(_parent)
    {
        partIndex = idx;
        ismulti = false;
        if (!isEmptyPTree(pt)) {
            if (pt->getPropInt("@num",idx+1)-1!=idx)
                WARNLOG("CPartDescriptor part index mismatch");
            overridename.set(pt->queryProp("@name"));
            if (overridename.isEmpty())
                overridename.clear();
            else
                ismulti = ::isMulti(overridename);
            props.setown(createPTreeFromIPT(pt));
            //props->removeProp("@num");        // keep these for legacy
            //props->removeProp("@name");
            props->removeProp("@node");
        }
        else
            props.setown(createPTree("Part"));
    }

    void set(unsigned idx, const char *_tail, IPropertyTree *pt)
    {
        partIndex = idx;
        setOverrideName(_tail);
        props.setown(pt?createPTreeFromIPT(pt):createPTree("Part"));
    }

    CPartDescriptor(CFileDescriptorBase &_parent, unsigned idx, MemoryBuffer &mb)
        : parent(_parent)
    {
        partIndex = idx;
        mb.read(overridename);
        if (overridename.isEmpty()) // shouldn't really need this
            overridename.clear();
        ismulti = ::isMulti(overridename);
        props.setown(createPTree(mb));
    }

    unsigned queryPartIndex()
    {
        return partIndex;
    }

    unsigned numCopies()
    {
        return parent.numCopies(partIndex);
    }

    virtual INode *queryNode(unsigned copy)
    {
        return parent.doQueryNode(partIndex,copy,(props&&props->hasProp("@rn"))?props->getPropInt("@rn"):(unsigned)-1);
    }

    virtual unsigned queryDrive(unsigned copy)
    {
        return parent.queryDrive(partIndex,copy);
    }

    INode *getNode(unsigned copy=0)
    {
        return LINK(queryNode(copy));
    }

    IPropertyTree &queryProperties()
    {
        return *props;
    }

    IPropertyTree *getProperties()
    {
        return props.get();
    }

    bool getCrc(unsigned &crc)
    {
        return getCrcFromPartProps(*parent.attr,*props,crc);
    }
    IFileDescriptor &queryOwner()
    {
        return parent.querySelf();
    }

    RemoteFilename &getFilename(unsigned copy, RemoteFilename &rfn)
    {
        if (ismulti) {
            RemoteMultiFilename rmfn;
            getMultiFilename(copy, rmfn);
            if (rmfn.ordinality()==1) {
                rfn.set(rmfn.item(0));
                return rfn;
            }
            throw MakeStringException(-1,"Remote Filename: Cannot resolve single part from wild/multi filename");
        }
        StringBuffer fullpath;
        getPath(fullpath,copy);
        rfn.setPath(queryNode(copy)->endpoint(),fullpath.str());
        return rfn;
    }

    StringBuffer &getPath(StringBuffer &path,unsigned copy)
    {
        StringBuffer tail;
        getTail(tail);
        if (!tail.length()||!isPathSepChar(tail.charAt(0))) {
            getDirectory(path,copy);
            addPathSepChar(path);
        }
        path.append(tail);
        return path;
    }

    StringBuffer &getTail(StringBuffer &name)
    {
        return parent.getPartTail(name,partIndex);
    }

    StringBuffer &getDirectory(StringBuffer &dir,unsigned copy)
    {
        return parent.getPartDirectory(dir,partIndex,copy);
    }

    bool isMulti()
    {
        return ismulti;
    }

    RemoteMultiFilename &getMultiFilename(unsigned copy, RemoteMultiFilename &rmfn)
    {
        if (ismulti) {
            rmfn.setEp(queryNode(copy)->endpoint());
            StringBuffer dir;
            parent.getPartDirectory(dir,partIndex,copy);
            StringBuffer tmp1;
            StringBuffer tmp2;
            splitDirMultiTail(overridename,tmp1,tmp2);
            rmfn.append(tmp2, dir);
        }
        else {
            RemoteFilename rfn;
            getFilename(copy,rfn);
            rmfn.append(rfn);
        }
        return rmfn;
    }

    void subserialize(MemoryBuffer &mb)
    {
        mb.append(overridename);
        props->serialize(mb);
    }

    bool subserializeTree(IPropertyTree *pt)
    {
        bool ret = false;
        if (props) {
            Owned<IAttributeIterator> attriter = props->getAttributes();
            ForEach(*attriter) {
                const char *an = attriter->queryName();
                if ((stricmp(an,"@num")!=0)&&(stricmp(an,"@name")!=0)) {
                    pt->setProp(an,attriter->queryValue());
                    ret = true;
                }
            }
            Owned<IPropertyTreeIterator> iter = props->getElements("*");
            ForEach(*iter) {
                ret = true;
                pt->addPropTree(iter->query().queryName(),createPTreeFromIPT(&iter->query()));
            }
        }
        if (!overridename.isEmpty()) {
            pt->setProp("@name",overridename);
            ret = true;
        }
        if (ret)
            pt->setPropInt("@num",partIndex+1);
        if ((partIndex==0)&&(parent.numParts()==1)) {  // more legacy
            SocketEndpoint ep = queryNode(0)->endpoint();
            StringBuffer tmp;
            if (!ep.isNull())
                pt->setProp("@node",ep.getUrlStr(tmp).str());
            if (overridename.isEmpty()&&!parent.partmask.isEmpty()) {
                expandMask(tmp.clear(), parent.partmask, 0, 1);
                pt->setProp("@name",tmp.str());
            }
        }
        return ret;
    }

    void setOverrideName(const char *_tail)
    {
        if (!_tail||!*_tail)
            overridename.clear();
        else
            overridename.set(_tail);
        ismulti = ::isMulti(_tail);
    }
    const char *queryOverrideName()
    {
        if (overridename.isEmpty())
            return NULL;
        return overridename;
    }


    void serialize(MemoryBuffer &mb)
    {
        parent.serializePart(mb,partIndex);
    }

    unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL)
    {
        return parent.copyClusterNum(partIndex,copy,replicate);
    }

    IReplicatedFile *getReplicatedFile()
    {
        IReplicatedFile *ret = createReplicatedFile();
        RemoteFilenameArray &copies = ret->queryCopies();

        unsigned nc = numCopies();
        for (unsigned copy=0;copy<nc;copy++) {
            RemoteFilename rfn;
            copies.append(getFilename(copy,rfn));
        }
        return ret;
    }

};




// --------------------------------------------------------

class CPartDescriptorArrayIterator : public CArrayIteratorOf<IPartDescriptor, IPartDescriptorIterator>
{
public:
    CPartDescriptorArrayIterator() : CArrayIteratorOf<IPartDescriptor, IPartDescriptorIterator>(array) { }
    CPartDescriptorArray array;
};

void getClusterInfo(IPropertyTree &pt, INamedGroupStore *resolver, unsigned flags, IArrayOf<IClusterInfo> &clusters)
{
    unsigned nc = pt.getPropInt("@numclusters");
    if (!nc) { // legacy format
        unsigned np = pt.getPropInt("@numparts");
        StringArray groups;
        getFileGroups(&pt,groups);
        unsigned gi = 0;
        do {
            Owned<IGroup> cgroup;
            const char *grp = (gi<groups.ordinality())?groups.item(gi):NULL;
            if (grp&&resolver)
                cgroup.setown(resolver->lookup(grp));
            // get nodes from parts if complete (and group 0)
            if (gi==0) { // don't assume lookup name correct!
                SocketEndpoint *eps = (SocketEndpoint *)calloc(np?np:1,sizeof(SocketEndpoint));
                MemoryBuffer mb;
                Owned<IPropertyTreeIterator> piter;
                if (pt.getPropBin("Parts",mb))
                    piter.setown(deserializePartAttrIterator(mb));
                else
                    piter.setown(pt.getElements("Part"));
                ForEach(*piter) {
                    IPropertyTree &cpt = piter->query();
                    unsigned num = cpt.getPropInt("@num");
                    if (num>np) {
                        eps = (SocketEndpoint *)checked_realloc(eps,num*sizeof(SocketEndpoint),np*sizeof(SocketEndpoint),-21);
                        memset(eps+np,0,(num-np)*sizeof(SocketEndpoint));
                        np = num;
                    }
                    const char *node = cpt.queryProp("@node");
                    if (node&&*node)
                        eps[num-1].set(node);
                }
                unsigned i=0;
                for (i=0;i<np;i++)
                    if (eps[i].isNull())
                        break;
                if (i==np) {
                    Owned<IGroup> ngrp = createIGroup(np,eps);
                    if (!cgroup.get()||(ngrp->compare(cgroup)!=GRbasesubset))
                        cgroup.setown(ngrp.getClear());
                }
                free(eps);
            }
            ClusterPartDiskMapSpec mspec;
            IClusterInfo *cluster = createClusterInfo(grp,cgroup,mspec,resolver);
            clusters.append(*cluster);
            gi++;
        } while (gi<groups.ordinality());
    }
    else {
        Owned<IPropertyTreeIterator> iter = pt.getElements("Cluster");
        ForEach(*iter)
            clusters.append(*deserializeClusterInfo(&iter->query(),resolver,flags));
    }
}


class CFileDescriptor:  public CFileDescriptorBase, implements ISuperFileDescriptor
{

    SocketEndpointArray *pending;   // for constructing cluster group
    bool setupdone;
    byte version;


    IFileDescriptor &querySelf()
    {
        return *this;
    }


    void openPending()
    {
        if (!pending) {
            pending = new SocketEndpointArray;
            if (setupdone)
                throw MakeStringException(-1,"IFileDescriptor - setup already done");
            setupdone = true;
            ClusterPartDiskMapSpec mspec;
            clusters.append(*createClusterInfo(NULL,NULL,mspec));
        }


    }

    void doClosePending()
    {
        // first sort out cluster
        unsigned np = parts.ordinality();
        unsigned n = pending->ordinality();
        assertex(clusters.ordinality());
        assertex(np>=n);
        if (n==0) {
            clusters.remove(clusters.ordinality()-1);
            WARNLOG("CFileDescriptor: removing empty cluster");
        }
        else {
            unsigned w;
            for (w=1;w<n;w++) {
                unsigned i;
                for (i=w;i<n;i++)
                    if (!pending->item(i).equals(pending->item(i%w)))
                        break;
                if (i==n)
                    break;

            }
            for (unsigned i=n;i>w;)
                pending->remove(--i);
            Owned<IGroup> newgrp = createIGroup(*pending);
            clusters.item(clusters.ordinality()-1).setGroup(newgrp);
        }
        delete pending;
        pending = NULL;
        if ((n==1)&&(isSpecialPath(part(0)->overridename)))
            return;
        // now look for a directory
        // this is a bit longwinded!
        // expand all tails
        StringBuffer tmp;
        if (!directory.isEmpty()) {
            StringBuffer fp;
            ForEachItemIn(i,parts) {

                CPartDescriptor *pt = part(i);
                if (!pt)
                    WARNLOG("Null part in pending file descriptor");
                else if (pt->isMulti()) {
                    assertex(!pt->overridename.isEmpty());
                    if (!isAbsolutePath(pt->overridename)) {        // assumes all multi are same
                        mergeDirMultiTail(directory,pt->overridename,fp.clear()); // assumes all multi are same
                        pt->setOverrideName(fp.str());
                    }
                }
                else {
                    RemoteFilename rfn;
                    pt->getFilename(0,rfn);
                    rfn.getLocalPath(fp.clear());
                    pt->setOverrideName(fp.str());
                }
            }
        }
        directory.clear();
        StringBuffer dir;
        // now find longest common dir (multi complicates this somewhat)
        CPartDescriptor &part0 = *part(0);
        bool multi = part0.isMulti();
        if (multi)
            splitDirMultiTail(part0.overridename,dir,tmp);
        else
            splitDirTail(part0.overridename,dir);
        if (dir.length()==0) {
            WARNLOG("CFileDescriptor cannot determine directory for file %s in '%s'",tracename.str(),part0.overridename.str());
        }
        else {
            const char *s = dir.str();
            for (unsigned i=1;i<np;i++) {
                CPartDescriptor &pt = *part(i);
                multi = pt.isMulti();
                StringBuffer tdir;      // would be easier without multi
                assertex(!pt.overridename.isEmpty());   // should have been set above
                if (multi) {
                    StringBuffer tmp;
                    splitDirMultiTail(pt.overridename,dir,tmp);
                }
                else
                    splitDirTail(part0.overridename,tdir);
                const char *t = tdir.str();
                const char *d = s;
                while (*d&&(*t==*d)) {
                    d++;
                    t++;
                }
                if (*t) { // not full match
                    while ((d!=s)&&!isPathSepChar(*(d-1)))
                        d--;
                    dir.setLength(d-s);
                    s = dir.str(); // paranoid
                    if (dir.length()<=2) // must be at least "/x/" or "d:\"
                        break; // no common dir
                }
            }
            if (dir.length()>2) {
                // now change all tails to relative
                StringBuffer relpath;
                for (unsigned i2=0;i2<np;i2++) {
                    CPartDescriptor &pt = *part(i2);
                    multi = pt.isMulti();
                    relpath.clear();
                    if (multi) {
                        removeRelativeMultiPath(pt.overridename,dir.str(),relpath);
                    }
                    else
                        relpath.append(splitRelativePath(pt.overridename,dir.str(),relpath));
                    pt.setOverrideName(relpath.str());
                }
                if ((dir.length()>1)&&(strcmp(dir.str()+1,":\\")!=0))
                    dir.setLength(dir.length()-1); // take off sep char
                directory.set(dir);
            }
        }
        // see if can create a partmask
        for (unsigned i=0;i<np;i++) {
            CPartDescriptor &pt = *part(i);
            if (pt.isMulti()) {
                partmask.clear();
                break;
            }
            if (!partmask.isEmpty()) {
                if (!matchesMask(pt.overridename,partmask,i,np)) {
                    partmask.clear();
                    if (i!=0)
                        break;
                }
            }
            if (partmask.isEmpty()&&!constructMask(partmask,pt.overridename,i,np))
                break;
        }
        if (partmask)
            for (unsigned i2=0;i2<np;i2++)
                part(i2)->setOverrideName(NULL);        // no longer need
    }

    inline void closePending()  // bit of a pain, but must be called at start of interrogation functions
    {
        if (pending)
            doClosePending();
    }


    StringBuffer &getPartTail(StringBuffer &name,unsigned idx)
    {
        unsigned n = numParts();
        if (idx<n) {
            CPartDescriptor &pt = *part(idx);
            if (!pt.overridename.isEmpty()) {
                if (isSpecialPath(pt.overridename))
                    return name.append(pt.overridename);
                if (pt.isMulti()) {
                    StringBuffer tmp;
                    splitDirMultiTail(pt.overridename,tmp,name);
                }
                else
                    name.append(pathTail(pt.overridename));
            }
            else if (!partmask.isEmpty())
                expandMask(name, pathTail(partmask), idx, n);
        }
        return name;
    }

    StringBuffer &getPartDirectory(StringBuffer &buf,unsigned idx,unsigned copy)
    {
        unsigned n = numParts();
        if (idx<n) {
            StringBuffer fullpath;
            StringBuffer tmp1;
            CPartDescriptor &pt = *part(idx);
            if (!pt.overridename.isEmpty()) {
                if (isSpecialPath(pt.overridename))
                    return buf;
                if (pt.isMulti()) {
                    StringBuffer tmpon; // bit messy but need to ensure dir put back on before removing!
                    const char *on = pt.overridename.get();
                    if (on&&*on&&!isAbsolutePath(on)&&!directory.isEmpty()) 
                        on = addPathSepChar(tmpon.append(directory)).append(on).str();
                    StringBuffer tmp2;
                    splitDirMultiTail(on,tmp1,tmp2);
                }
                else
                    splitDirTail(pt.overridename,tmp1);
                if (directory.isEmpty()||(isAbsolutePath(tmp1.str())||(stdIoHandle(tmp1.str())>=0)))
                    fullpath.swapWith(tmp1);
                else {
                    fullpath.append(directory);
                    if (fullpath.length())
                        addPathSepChar(fullpath);
                    fullpath.append(tmp1);
                }
            }
            else if (!partmask.isEmpty()) {
                fullpath.append(directory);
                if (containsPathSepChar(partmask)) {
                    if (fullpath.length())
                        addPathSepChar(fullpath);
                    splitDirTail(partmask,fullpath);
                }
            }
            else
                fullpath.append(directory);
            replaceClusterDir(idx,copy, fullpath);
            StringBuffer baseDir, repDir;
            unsigned lcopy;
            IClusterInfo * cluster = queryCluster(idx,copy,lcopy);
            if (cluster)
            {
                DFD_OS os = SepCharBaseOs(getPathSepChar(fullpath));
                cluster->getBaseDir(baseDir, os);
                cluster->getReplicateDir(repDir, os);
            }
            setReplicateFilename(fullpath,queryDrive(idx,copy),baseDir.str(),repDir.str());
            if ((fullpath.length()>3)&&isPathSepChar(fullpath.charAt(fullpath.length()-1)))
                fullpath.setLength(fullpath.length()-1);
            if (buf.length())
                buf.append(fullpath);
            else
                buf.swapWith(fullpath);
        }
        return buf;
    }


    IClusterInfo *queryCluster(unsigned partno,unsigned copy, unsigned &lcopy)
    {
        closePending();
        unsigned n=clusters.ordinality();
        unsigned i=0;
        bool c = 0;
        while (i<n) {
            unsigned mc = numClusterCopies(i,partno);
            if (copy<mc) {
                lcopy = copy;
                return &clusters.item(i);
            }
            copy -= mc;
            i++;
        }
        return NULL;
    }

    IClusterInfo *queryCluster(const char *_clusterName)
    {
        StringAttr clusterName = _clusterName;
        clusterName.toLowerCase();
        StringBuffer name;
        ForEachItemIn(c, clusters)
        {
            if (0 == strcmp(clusters.item(c).getClusterLabel(name.clear()).str(), clusterName))
                return &clusters.item(c);
        }
        return NULL;
    }

    void replaceClusterDir(unsigned partno,unsigned copy, StringBuffer &path)
    {
        // assumes default dir matches one of clusters
        closePending();
        if (path.length()<3)
            return;
        const char *ds = path.str();
        unsigned nc = clusters.ordinality();
        if (nc<=1)
            return; // not much can do
        StringAttr matched;
        StringAttr toadd;
        unsigned i=0;
        bool c = 0;
        int cp = (int)copy;
        while (i<nc) {
            StringBuffer dcmp;
            clusters.item(i).getBaseDir(dcmp,SepCharBaseOs(getPathSepChar(ds)));    // no trailing sep
            const char *t = dcmp.str();
            const char *d = ds;
            while (*d&&(*t==*d)) {
                d++;
                t++;
            }
            if (!*t&&(!*d||isPathSepChar(*d))&&(dcmp.length()>matched.length()))
                matched.set(dcmp);
            unsigned mc = numClusterCopies(i,partno);
            if ((cp>=0)&&(copy<mc))
                toadd.set(dcmp);
            copy -= mc;
            i++;
        }
        if (!matched.isEmpty()&&!toadd.isEmpty()&&(strcmp(matched,toadd)!=0)) {
            StringBuffer tmp(toadd);
            tmp.append(ds+matched.length());
            path.swapWith(tmp);
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    CFileDescriptor(MemoryBuffer &mb, IArrayOf<IPartDescriptor> *partsret, UnsignedArray **subcounts=NULL, bool *_interleaved=NULL)
    {
        // bit fiddly
        if (subcounts)
            *subcounts = NULL;
        pending = NULL;
        setupdone = true;
        mb.read(version);
        if ((version!=SERIALIZATION_VERSION)&&(version!=SERIALIZATION_VERSION2)) // check seialization matched
            throw MakeStringException(-1,"FileDescriptor serialization version mismatch %d/%d",(int)SERIALIZATION_VERSION,(int)version);
        mb.read(tracename);
        mb.read(directory);
        mb.read(partmask);
        unsigned n;
        mb.read(n);
        for (unsigned i1=0;i1<n;i1++)
            clusters.append(*deserializeClusterInfo(mb));
        unsigned partidx;
        mb.read(partidx);   // -1 if all parts, -2 if multiple parts
        mb.read(n); // numparts
        CPartDescriptor *part;
        if (partidx==(unsigned)-2) {
            UnsignedArray pia;
            unsigned pi;
            loop {
                mb.read(pi);
                if (pi==(unsigned)-1)
                    break;
                pia.append(pi);
            }
            for (unsigned i3=0;i3<n;i3++)
                parts.append(NULL);
            ForEachItemIn(i4,pia) {
                unsigned p = pia.item(i4);
                if (p<n) {
                    part = new CPartDescriptor(*this,p,mb);
                    parts.replace(part,p);
                }
            }
            if (partsret) {
                ForEachItemIn(i5,pia) {
                    unsigned p = pia.item(i5);
                    if (p<parts.ordinality()) {
                        CPartDescriptor *pt = (CPartDescriptor *)parts.item(p);
                        partsret->append(*LINK(pt));
                    }
                }
            }

        }
        else {
            for (unsigned i2=0;i2<n;i2++) {
                if ((partidx==(unsigned)-1)||(partidx==i2)) {
                    part = new CPartDescriptor(*this,i2,mb);
                    if (partsret)
                        partsret->append(*LINK(part));
                }
                else
                    part = NULL; // new CPartDescriptor(*this,i2,NULL);
                parts.append(part);
            }
        }
        attr.setown(createPTree(mb));
        if (!attr)
            attr.setown(createPTree("Attr")); // doubt can happen
        if (version==SERIALIZATION_VERSION2) {
            if (subcounts)
                *subcounts = new UnsignedArray;
            unsigned n;
            mb.read(n);
            while (n) {
                unsigned np;
                mb.read(np);
                if (subcounts)
                    (*subcounts)->append(np);
                n--;
            }
            bool interleaved;
            mb.read(interleaved);
            if (_interleaved)
                *_interleaved = interleaved;
        }
    }



    CFileDescriptor(IPropertyTree *tree, INamedGroupStore *resolver, unsigned flags)
    {
        pending = NULL;
        if ((flags&IFDSF_ATTR_ONLY)||!tree) {
            if (!tree)
                tree = createPTree("Attr");
            attr.setown(tree);
            setupdone = false;
            return;
        }
        else
            setupdone = true;
        IPropertyTree &pt = *tree;
        tracename.set(pt.queryProp("@trace"));
        directory.set(pt.queryProp("@directory"));
        partmask.set(pt.queryProp("@partmask"));
        unsigned np = pt.getPropInt("@numparts");
        StringBuffer query;
        IPropertyTree **trees = NULL;
        Owned<IPropertyTreeIterator> piter;
        MemoryBuffer mb;
        IPropertyTree *at = pt.queryPropTree("Attr");
        getClusterInfo(pt,resolver,flags,clusters);
        offset_t totalsize = (offset_t)-1;
        if (flags&IFDSF_EXCLUDE_PARTS) {
            for (unsigned i2=0;i2<np;i2++)
                parts.append(new CPartDescriptor(*this,i2,NULL));
        }
        else {
            if (!at||(at->getPropInt64("@size",-1)==-1))
                totalsize = 0;
            if ((piter.get()&&mb.length())||pt.getPropBin("Parts",mb)) {
                if (!piter.get())
                    piter.setown(deserializePartAttrIterator(mb));
                unsigned i2=0;
                ForEach(*piter) {
                    if (totalsize!=(offset_t)-1) {
                        offset_t sz = piter->query().getPropInt64("@size",-1);
                        if (sz!=(offset_t)-1)
                            totalsize += sz;
                        else
                            totalsize = (offset_t)-1;
                    }
                    parts.append(new CPartDescriptor(*this,i2++,&piter->query()));
                }
            }
            else {  // parts may not be in order
                IArrayOf<IPropertyTree> trees;
                if (!piter.get())
                    piter.setown(pt.getElements("Part"));
                ForEach(*piter) {
                    IPropertyTree &cpt = piter->query();
                    unsigned num = cpt.getPropInt("@num");
                    if (!num)
                        continue;
                    while (num>trees.ordinality()+1)
                        trees.append(*createPTree("Part"));
                    cpt.Link();
                    if (num>trees.ordinality())
                        trees.append(cpt);
                    else
                        trees.replace(cpt,num-1);
                }
                for (unsigned i2=0;i2<np;i2++) {
                    if (totalsize!=(offset_t)-1) {
                        offset_t sz = (i2<trees.ordinality())?(offset_t)trees.item(i2).getPropInt64("@size",-1):(offset_t)-1;
                        if (sz!=(offset_t)-1)
                            totalsize += sz;
                        else
                            totalsize = (offset_t)-1;
                    }
                    parts.append(new CPartDescriptor(*this,i2,(i2<trees.ordinality())?&trees.item(i2):NULL));
                }
            }
        }
        piter.clear();
        if (at)
            attr.setown(createPTreeFromIPT(at));
        else
            attr.setown(createPTree("Attr"));
        if (totalsize!=(offset_t)-1)
            attr->setPropInt64("@size",totalsize);
    }

    void serializePart(MemoryBuffer &mb,unsigned partidx)
    {
        serializeParts(mb,&partidx,1);
    }

    void serializeParts(MemoryBuffer &mb,unsigned *partlist, unsigned nparts);

    void serializeParts(MemoryBuffer &mb,UnsignedArray &partlist)
    {
        serializeParts(mb,partlist.getArray(),partlist.ordinality());
    }

    void serialize(MemoryBuffer &mb)
    {
        serializePart(mb,(unsigned)-1);
    }

    void serializeTree(IPropertyTree &pt,unsigned flags)
    {
        closePending();
//      if (!tracename.isEmpty())
//          pt.setProp("@trace",tracename);             // don't include trace name in tree (may revisit later)
        if (!directory.isEmpty())
            pt.setProp("@directory",directory);
        if (!partmask.isEmpty())
            pt.setProp("@partmask",partmask);
        unsigned n = clusters.ordinality();
        pt.setPropInt("@numclusters",n);
        unsigned cn = 0;

// JCSMORE - afaics, IFileDescriptor @group is no longer used
        StringBuffer grplist;
        ForEachItemIn(i1,clusters) {
            Owned<IPropertyTree> ct = createPTree("Cluster");
            clusters.item(i1).serializeTree(ct,flags);
            if (!isEmptyPTree(ct)) {
                const char *cname = ct->queryProp("@name");
                if (cname&&*cname) {
                    if (grplist.length())
                        grplist.append(',');
                    grplist.append(cname);
                }
                pt.addPropTree("Cluster",ct.getClear());
            }
            else
                WARNLOG("CFileDescriptor::serializeTree - empty cluster");
        }
        if (grplist.length())
            pt.setProp("@group",grplist.str());
        else
            pt.removeProp("@group");
/// ^^

        n = numParts();
        pt.setPropInt("@numparts",n);
        if ((flags&IFDSF_EXCLUDE_PARTS)==0) {
            if ((n==1)||((flags&CPDMSF_packParts)==0)) {
                for (unsigned i2=0;i2<n;i2++) {
                    Owned<IPropertyTree> p = createPTree("Part");
                    if (part(i2)->subserializeTree(p))
                        pt.addPropTree("Part",p.getClear());
                }
            }
            else {
                MemoryBuffer mb;
                for (unsigned i2=0;i2<n;i2++) {
                    // this seems a bit excessive in conversions
                    Owned<IPropertyTree> p = createPTree("Part");
                    part(i2)->subserializeTree(p);
                    serializePartAttr(mb,p);
                }
                pt.setPropBin("Parts",mb.length(),mb.toByteArray());
            }
        }
        IPropertyTree *t = &queryProperties();
        if (!isEmptyPTree(t))
            pt.addPropTree("Attr",createPTreeFromIPT(t));
    }

    IPropertyTree *getFileTree(unsigned flags)
    {
        Owned<IPropertyTree> ret = createPTree(queryDfsXmlBranchName(DXB_File));
        serializeTree(*ret,flags);
        return ret.getClear();
    }



    virtual ~CFileDescriptor()
    {
        closePending();             // not sure strictly needed
        ForEachItemInRev(p, parts)
            delpart(p);
    }

    void setDefaultDir(const char *dirname)
    {
        const char *s=dirname;
        size32_t l = strlen(s);
        char sc = 0;
        if ((l>1)&&(isPathSepChar(dirname[l-1]))&&(strcmp(dirname+1,":\\")!=0)) {
            l--;
            sc = dirname[l];
        }
        if (l&&!isAbsolutePath(dirname)) { // support cross-OS
            // assume relative path on same OS
            if (!sc)
                sc = getPathSepChar(dirname);
            StringBuffer tmp;
            tmp.append(queryBaseDirectory(grp_unknown, 0, SepCharBaseOs(sc))).append(sc).append(s);
            directory.set(tmp.str());
        }
        else
            directory.set(s,l);
    }

    int getReplicateOffset(unsigned clusternum)
    {
        closePending();
        if (clusternum>=clusters.ordinality())
            return 1;
        return clusters.item(clusternum).queryPartDiskMapping().replicateOffset;
    }

    CPartDescriptor *part(unsigned idx)
    {
        CPartDescriptor *ret = (CPartDescriptor *)parts.item(idx);
        if (!ret) { // this is not normally expected!
            ret = new CPartDescriptor(*this,idx,NULL);
            parts.replace(ret,idx);
        }
        return ret;
    }

    void delpart(unsigned idx)
    {
        CPartDescriptor *p = (CPartDescriptor *)parts.item(idx);
        delete p;
        parts.remove(idx);
    }

    void doSetPart(unsigned idx, const SocketEndpoint &ep, const char *filename, IPropertyTree *pt)
    {
        // setPart from ep/node ignores port in ep
        openPending();
        while (parts.ordinality()<=idx) {
            SocketEndpoint nullep;
            parts.append(new CPartDescriptor(*this,idx,NULL));
            pending->append(nullep);
        }
        CPartDescriptor &p = *part(idx);
        p.set(idx,filename,pt);
        if (idx>=pending->ordinality())
            ERRLOG("IFileDescriptor setPart called after cluster finished");
        else {
            SocketEndpoint &pep = pending->element(idx);
            if (pep.isNull())
                pep=ep;
            else
                ERRLOG("IFileDescriptor setPart called twice for same part");
        }
    }

    void setPart(unsigned idx, INode *node, const char *filename, IPropertyTree *pt)
    {
        if (node)
            setPart(idx,node->endpoint(),filename,pt); // ignore port
    }


    void setPart(unsigned idx, const IpAddress &ip, const char *filename, IPropertyTree *pt)
    {
        SocketEndpoint ep(0,ip);
        doSetPart(idx,ep,filename,pt);
    }

    void setPart(unsigned idx, const RemoteFilename &name, IPropertyTree *pt)
    {
        StringBuffer localname;
        name.getLocalPath(localname);
        SocketEndpoint ep = name.queryEndpoint();
        doSetPart(idx,ep,localname.str(),pt);
    }



    void setTraceName(const char *trc)
    {
        tracename.set(trc);
    }

    unsigned numClusterCopies(unsigned cnum,unsigned partnum)
    {
        IClusterInfo &cluster = clusters.item(cnum);
        IGroup *grp = cluster.queryGroup();
        return cluster.queryPartDiskMapping().numCopies(partnum,grp?grp->ordinality():1,numParts());

    }

    unsigned numCopies(unsigned partnum)
    {
        closePending();
        unsigned ret = 0;
        ForEachItemIn(i,clusters)
            ret += numClusterCopies(i,partnum);
        return ret;
    }


    INode *getNode(unsigned partidx,unsigned copy)
    {
        INode *ret = queryNode(partidx,copy);
        return LINK(ret);
    }

    INode *doQueryNode(unsigned idx,unsigned copy,unsigned rn)
    {
        closePending();
        unsigned lcopy;
        IClusterInfo * cluster = queryCluster(idx,copy,lcopy);
        if (!cluster)
            return queryNullNode();
        if ((copy==1)&&(rn!=(unsigned)-1)) {
            IGroup *group = cluster->queryGroup();
            if (group&&rn<group->ordinality())
                return &group->queryNode((rank_t)rn);
        }
        return cluster->queryNode(idx,numParts(),lcopy);
    }

    unsigned queryDrive(unsigned idx,unsigned copy)
    {
        closePending();
        unsigned lcopy;
        IClusterInfo * cluster = queryCluster(idx,copy,lcopy);
        if (!cluster)
            return 0;
        return cluster->queryDrive(idx,numParts(),lcopy);
    }

    INode *queryNode(unsigned idx,unsigned copy)
    {
        closePending();
        if (idx<numParts())
            return part(idx)->queryNode(copy);
        return NULL;
    }


    RemoteFilename &getFilename(unsigned idx, unsigned copy, RemoteFilename &rfn)
    {
        closePending();
        return part(idx)->getFilename(copy, rfn);
    }


    StringBuffer &getTraceName(StringBuffer &str)
    {
        closePending();
        return str.append(tracename);
    }

    virtual IPropertyTree *getProperties()
    {
        closePending();
        return attr.getLink();
    }

    IPropertyTree &queryProperties()
    {
        closePending();
        return *attr.get();
    }

    bool isMulti(unsigned partidx=(unsigned)-1)
    {
        closePending();
        if (partidx==(unsigned)-1) {
            for(partidx=0; partidx<numParts(); partidx++)
                if (part(partidx)->isMulti())
                    return true;
            return false;
        }
        return ((partidx<numParts()) && part(partidx)->isMulti());
    }

    RemoteMultiFilename &getMultiFilename(unsigned partidx,unsigned cpy, RemoteMultiFilename &rfn)
    {
        closePending();
        return part(partidx)->getMultiFilename(cpy, rfn);
    }


    IPartDescriptor *getPart(unsigned idx)
    {
        IPartDescriptor *ret = queryPart(idx);
        return LINK(ret);
    }

    IPartDescriptor *queryPart(unsigned idx)
    {
        closePending();
        if (idx<numParts())
            return part(idx);
        return NULL;
    }

    IPartDescriptorIterator *getIterator()
    {
        closePending();
        CPartDescriptorArrayIterator *iter = new CPartDescriptorArrayIterator();
        unsigned n=0;
        for (; n<numParts(); n++) iter->array.append(*getPart(n));
        return iter;
    }
    const char *queryKind()
    {
        return queryProperties().queryProp("@kind");
    }
    bool isGrouped()
    {
        return queryProperties().getPropBool("@grouped");
    }
    bool isCompressed(bool *blocked=NULL)
    {
        return ::isCompressed(queryProperties(), blocked);
    }


    const char *queryDefaultDir()
    {
        closePending();
        return directory;
    }

    void setPartMask(const char *mask)
    {
        partmask.set(mask);
    }

    unsigned addCluster(const char *name,IGroup *grp,const ClusterPartDiskMapSpec &map)
    {
        closePending();
        IClusterInfo * cluster = createClusterInfo(name,grp,map);
        clusters.append(*cluster);
        return clusters.ordinality()-1;
    }

    unsigned addCluster(IGroup *grp,const ClusterPartDiskMapSpec &map)
    {
        return addCluster(NULL,grp,map);
    }

    void endCluster(ClusterPartDiskMapSpec &map)
    {
        closePending();
        if (clusters.ordinality())
            clusters.item(clusters.ordinality()-1).queryPartDiskMapping() = map;
    }

    const char *queryPartMask()
    {
        closePending();
        return partmask;
    }

    IGroup *getGroup()
    {
        IGroup *g = queryClusterGroup(0);
        return LINK(g);
    }

    unsigned numParts()
    {
        closePending();
        return parts.ordinality();
    }

    void setNumParts(unsigned numparts)
    {
        closePending();
        while (parts.ordinality()<numparts)
            parts.append(new CPartDescriptor(*this,parts.ordinality(),NULL));
        while (parts.ordinality()>numparts)
            delpart(parts.ordinality()-1);
    }

    unsigned numClusters()
    {
        closePending();
        return clusters.ordinality();
    }

    unsigned copyClusterNum(unsigned partidx, unsigned copy,unsigned *replicate=NULL)
    {
        unsigned lcopy=0;
        IClusterInfo * cluster = queryCluster(partidx,copy,lcopy);
        if (replicate)
            *replicate = lcopy;
        if (!cluster)
            return NotFound;
        // bit silly finding again
        return clusters.find(*cluster);
    }

    ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).queryPartDiskMapping();
    }

    IGroup *queryClusterGroup(unsigned clusternum)
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).queryGroup();
    }


    void setClusterGroup(unsigned clusternum,IGroup *grp)
    {
        closePending();
        assertex(clusternum<numClusters());
        clusters.item(clusternum).setGroup(grp);
    }

    StringBuffer &getClusterGroupName(unsigned clusternum,StringBuffer &ret,IGroupResolver *resolver)
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).getGroupName(ret,resolver);
    }

    void setClusterGroupName(unsigned clusternum,const char *name)
    {
        closePending();
        assertex(clusternum<numClusters());
        clusters.item(clusternum).setGroupName(name);
    }

    StringBuffer &getClusterLabel(unsigned clusternum,StringBuffer &ret)
        // either roxie label or node group name
    {
        closePending();
        assertex(clusternum<numClusters());
        return clusters.item(clusternum).getClusterLabel(ret);
    }

    void setClusterOrder(StringArray &names,bool exclusive)
    {
        closePending();
        unsigned done = 0;
        StringBuffer cname;
        ForEachItemIn(i,names)
        {
            StringAttr name = names.item(i);
            name.toLowerCase();
            for (unsigned j=done;j<clusters.ordinality();j++)
            {
                clusters.item(j).getClusterLabel(cname.clear());
                if (strcmp(cname.str(),name)==0)
                {
                    if (done!=j)
                        clusters.swap(done,j);
                    done++;
                    break;
                }
            }
        }
        if (exclusive)
        {
            if (!done)
                done = 1;
            StringAttr oldDefaultDir;
            StringBuffer baseDir1;
            while (clusters.ordinality()>done)
            {
                clusters.item(clusters.ordinality()-1).getBaseDir(baseDir1.clear(),SepCharBaseOs(getPathSepChar(directory)));

                // if baseDir is leading component this file's default directory..
                if (!oldDefaultDir.length() && directory.length()>=baseDir1.length() && 0==strncmp(directory, baseDir1, baseDir1.length()) &&
                    (directory.length()==baseDir1.length() || isPathSepChar(directory[baseDir1.length()])))
                    oldDefaultDir.set(baseDir1.str());
                clusters.remove(clusters.ordinality()-1);
            }
            if (oldDefaultDir.length() && clusters.ordinality())
            {
                StringBuffer baseDir2;
                clusters.item(0).getBaseDir(baseDir2.clear(), SepCharBaseOs(getPathSepChar(directory)));
                StringBuffer newDir(baseDir2.str());
                if (directory.length()>oldDefaultDir.length())
                    newDir.append(directory.get()+oldDefaultDir.length());
                directory.set(newDir.str());
            }
        }
    }

    virtual void ensureReplicate()
    {
        for (unsigned clusterIdx = 0; clusterIdx<numClusters(); clusterIdx++)
            queryPartDiskMapping(clusterIdx).ensureReplicate();
    }

    ISuperFileDescriptor *querySuperFileDescriptor()
    {
        return NULL;
    }

    bool mapSubPart(unsigned superpartnum, unsigned &subfile, unsigned &subpartnum)
    {
        // shouldn't get called ever
        subpartnum = superpartnum;
        subfile = 0;
        return true;
    }

    void setSubMapping(UnsignedArray &_subcounts, bool _interleaved)
    {
        UNIMPLEMENTED_X("setSubMapping called from CFileDescriptor!");
    }

    unsigned querySubFiles()
    {
        UNIMPLEMENTED_X("querySubFiles called from CFileDescriptor!");
    }
};

class CSuperFileDescriptor:  public CFileDescriptor
{
    UnsignedArray *subfilecounts;
    bool interleaved; 
public:

    IMPLEMENT_IINTERFACE;

    CSuperFileDescriptor(MemoryBuffer &mb, IArrayOf<IPartDescriptor> *partsret) 
        : CFileDescriptor(mb,partsret,&subfilecounts,&interleaved)
    {
    }

    CSuperFileDescriptor(IPropertyTree *attr)
        : CFileDescriptor(attr,NULL,IFDSF_ATTR_ONLY)    // only support attr here
    {
        subfilecounts = NULL;
    }


    virtual ~CSuperFileDescriptor()
    {
        delete subfilecounts;
    }

    ISuperFileDescriptor *querySuperFileDescriptor()
    {
        return this;
    }

    bool mapSubPart(unsigned superpartnum, unsigned &subfile, unsigned &subpartnum)
    {
        subpartnum = superpartnum;
        subfile = 0;
        if (!subfilecounts)  // its a file!
            return true;
        if (interleaved) { 
            unsigned p = 0;
            unsigned f = 0;
            bool found = false;
            loop {
                if (f==subfilecounts->ordinality()) {
                    if (!found)
                        break; // no more
                    found = false;
                    p++;
                    f = 0;
                }
                if (p<subfilecounts->item(f)) {
                    if (!superpartnum) {
                        subfile = f;
                        subpartnum = p;
                        return true;
                    }
                    superpartnum--;
                    found = true;
                }
                f++;
            }
        }
        else { // sequential
            while (subfile<subfilecounts->ordinality()) {
                if (subpartnum<subfilecounts->item(subfile)) 
                    return true;
                subpartnum -= subfilecounts->item(subfile);
                subfile++;
            }
        }
        return false;
    }

    void setSubMapping(UnsignedArray &_subcounts, bool _interleaved)
    {
        interleaved = _interleaved;
        if (_subcounts.ordinality()) {
            if (subfilecounts)
                subfilecounts->kill();
            else
                subfilecounts = new UnsignedArray;
            ForEachItemIn(i,_subcounts)
                subfilecounts->append(_subcounts.item(i));
        }
        else {
            delete subfilecounts;
            subfilecounts = NULL;
        }
    }

    unsigned querySubFiles()
    {
        if (!subfilecounts)  // its a file!
            return 1;
        return subfilecounts->ordinality();
    }

    void serializeSub(MemoryBuffer &mb)
    {
        if (subfilecounts) {
            unsigned count = subfilecounts->ordinality();
            mb.append(count);
            ForEachItemIn(i,*subfilecounts)
                mb.append(subfilecounts->item(i));
        }
        else
            mb.append((unsigned)0);
        mb.append(interleaved);
    }
};


void CFileDescriptor::serializeParts(MemoryBuffer &mb,unsigned *partlist, unsigned nparts)
{
    closePending();
    ISuperFileDescriptor *isdesc = querySuperFileDescriptor();
    CSuperFileDescriptor *sdesc = isdesc?(QUERYINTERFACE(isdesc,CSuperFileDescriptor)):NULL;
    mb.append(sdesc?SERIALIZATION_VERSION2:SERIALIZATION_VERSION);
    mb.append(tracename);
    mb.append(directory);
    mb.append(partmask);
    // first clusters
    unsigned n = clusters.ordinality();
    mb.append(n);
    ForEachItemIn(i1,clusters)
        clusters.item(i1).serialize(mb);
    n = numParts();
    if (nparts==1) {
        unsigned pi = *partlist;
        mb.append(pi).append(n);
        if (pi==(unsigned)-1) {
            for (unsigned i2=0;i2<n;i2++)
                part(i2)->subserialize(mb);
        }
        else if (pi<n)
            part(pi)->subserialize(mb);
    }
    else {
        mb.append((unsigned)-2).append(n);  // -2 is for multiple
        for (unsigned i3=0;i3<nparts;i3++)
            mb.append(partlist[i3]);
        mb.append((unsigned)-1); // end of list
        for (unsigned i4=0;i4<nparts;i4++)
            part(partlist[i4])->subserialize(mb);
    }
    queryProperties().serialize(mb);
    if (sdesc)
        sdesc->serializeSub(mb);
}


IFileDescriptor *createFileDescriptor(IPropertyTree *tree)
{
    return new CFileDescriptor(tree,NULL,IFDSF_ATTR_ONLY);
}

ISuperFileDescriptor *createSuperFileDescriptor(IPropertyTree *tree)
{
    return new CSuperFileDescriptor(tree);
}


IFileDescriptor *createFileDescriptor()
{
    return new CFileDescriptor(NULL,NULL,0);
}

static IFileDescriptor *_createExternalFileDescriptor(const char *_logicalname, bool lookup)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    //authentication already done
    SocketEndpoint ep;
    Owned<IGroup> group;
    if (!logicalname.getEp(ep))
    {
        StringBuffer grp;
        if (logicalname.getGroupName(grp).length()==0)
            throw MakeStringException(-1,"missing node in external file name (%s)",logicalname.get());
        group.setown(queryNamedGroupStore().lookup(grp.str()));
        if (!group)
            throw MakeStringException(-1,"cannot resolve node %s in external file name (%s)",grp.str(),logicalname.get());
        ep = group->queryNode(0).endpoint();
    }

    bool iswin=false;
    bool usedafs;
    switch (getDaliServixOs(ep))
    {
    case DAFS_OSwindows:
        iswin = true;
        // fall through
    case DAFS_OSlinux:
    case DAFS_OSsolaris:
        usedafs = ep.port||!ep.isLocal();
        break;
    default:
#ifdef _WIN32
        iswin = true;
#else
        iswin = false;
#endif
        usedafs = false;
        break;
    }

    //rest is local path
    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    StringBuffer dir;
    StringBuffer tail;
    IException *e=NULL;
    if (!logicalname.getExternalPath(dir,tail,iswin,&e))
    {
        if (e)
            throw e;
        return NULL;
    }
    fileDesc->setDefaultDir(dir.str());
    unsigned n = group.get()?group->ordinality():1;
    StringBuffer partname;
    CDateTime modTime;
    StringBuffer modTimeStr;
    for (unsigned i=0;i<n;i++)
    {
        if (group.get())
            ep = group->queryNode(i).endpoint();
        partname.clear();
        partname.append(dir);
        const char *s = tail.str();
        bool isspecial = (*s=='>');
        if (isspecial)
            partname.append(s);
        else
        {
            while (*s)
            {
                if (memicmp(s,"$P$",3)==0)
                {
                    partname.append(i+1);
                    s += 3;
                }
                else if (memicmp(s,"$N$",3)==0)
                {
                    partname.append(n);
                    s += 3;
                }
                else
                    partname.append(*(s++));
            }
        }
        if (!ep.port&&usedafs)
            ep.port = getDaliServixPort();
        RemoteFilename rfn;
        rfn.setPath(ep,partname.str());
        if (!isspecial&&(memcmp(partname.str(),"/$/",3)!=0)&&(memcmp(partname.str(),"\\$\\",3)!=0)) // don't get date on external data
        {
            try
            {
                Owned<IFile> file = createIFile(rfn);
                CDateTime dt;
                if (file&&file->getTime(NULL,&dt,NULL))
                {
                    if ((0 == modTimeStr.length())||(dt.compareDate(modTime)>0))
                    {
                        modTime.set(dt);
                        modTime.getString(modTimeStr);
                    }
                }
            }
            catch (IException *e)
            {
                EXCLOG(e,"CDistributedFileDirectory::createExternal");
                e->Release();
            }
        }
        if (lookup)
        {
            OwnedIFile iFile = createIFile(rfn);
            if (!iFile->exists())
                return NULL; // >=1 part does not exist.
        }
        if (modTimeStr.length())
        {
            Owned<IPropertyTree> part = createPTree("Part");
            part->setProp("@modified", modTimeStr.str());
            fileDesc->setPart(i, rfn, part);
        }
        else
            fileDesc->setPart(i, rfn);
    }
    fileDesc->queryPartDiskMapping(0).defaultCopies = DFD_NoCopies;
    return fileDesc.getClear();
}

IFileDescriptor *createExternalFileDescriptor(const char *logicalname)
{
    return _createExternalFileDescriptor(logicalname, false);
}
IFileDescriptor *getExternalFileDescriptor(const char *logicalname)
{
    return _createExternalFileDescriptor(logicalname, true);
}

inline void moveProp(IPropertyTree *to,IPropertyTree *from,const char *name)
{
    const char *p = from->queryProp(name);
    if (p&&*p) {
        to->setProp(name,p);
        from->removeProp(name);
    }
}

static CFileDescriptor * doDeserializePartFileDescriptors(MemoryBuffer &mb,IArrayOf<IPartDescriptor> *parts)
{
    size32_t savepos = mb.getPos();
    byte version;
    mb.read(version);
    mb.reset(savepos);
    if (version==SERIALIZATION_VERSION2) // its super
        return new CSuperFileDescriptor(mb,parts);
    return new CFileDescriptor(mb,parts);
}


extern da_decl void deserializePartFileDescriptors(MemoryBuffer &mb,IArrayOf<IPartDescriptor> &parts)
{
    Owned<CFileDescriptor> parent = doDeserializePartFileDescriptors(mb,&parts);
}


IPartDescriptor *deserializePartFileDescriptor(MemoryBuffer &mb)
{
    IArrayOf<IPartDescriptor> parts;
    Owned<CFileDescriptor> parent = doDeserializePartFileDescriptors(mb,&parts);
    if (parts.ordinality()!=1)
        ERRLOG("deserializePartFileDescriptor deserializing multiple parts not single part");
    if (parts.ordinality()==0)
        return NULL;
    return LINK(&parts.item(0));
}


IFileDescriptor *createFileDescriptor(const char *lname,IGroup *grp,IPropertyTree *tree,DFD_OS os,unsigned width)
{
    // only handles 1 copy
    IFileDescriptor *res = createFileDescriptor(tree);
    res->setTraceName(lname);
    StringBuffer dir;
    makePhysicalPartName(lname, 0, 0, dir,false,os);
    res->setDefaultDir(dir.str());
    if (width==0)
        width = grp->ordinality();
    StringBuffer s;
    for (unsigned i=0;i<width;i++) {
        makePhysicalPartName(lname, i+1, width, s.clear(),false,os);
        RemoteFilename rfn;
        rfn.setPath(grp->queryNode(i%grp->ordinality()).endpoint(),s.str());
        res->setPart(i,rfn,NULL);
    }
    ClusterPartDiskMapSpec map; // use defaults
    map.defaultCopies = DFD_DefaultCopies;
    res->endCluster(map);
    return res;
}



IFileDescriptor *deserializeFileDescriptor(MemoryBuffer &mb)
{
    return doDeserializePartFileDescriptors(mb,NULL);
}

IFileDescriptor *deserializeFileDescriptorTree(IPropertyTree *tree, INamedGroupStore *resolver, unsigned flags)
{
    return new CFileDescriptor(tree, resolver, flags);
}


inline bool validFNameChar(char c)
{
    static const char *invalids = "*\"/:<>?\\|";
    return (c>=32 && c<127 && !strchr(invalids, c));
}

static const char * defaultWindowsBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS] =
    {
            { "c:\\thordata", "d:\\thordata" },
            { "c:\\thordata", "d:\\thordata" },
            { "c:\\roxiedata", "d:\\roxiedata" },
            { "c:\\hthordata", "d:\\hthordata" },
            { "c:\\hthordata", "d:\\hthordata" },
    };
static const char * defaultUnixBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS] =
    {
        { "/var/lib/HPCCSystems/hpcc-data/thor", "/var/lib/HPCCSystems/hpcc-mirror/thor" },
        { "/var/lib/HPCCSystems/hpcc-data/thor", "/var/lib/HPCCSystems/hpcc-mirror/thor" },
        { "/var/lib/HPCCSystems/hpcc-data/roxie", "/var/lib/HPCCSystems/hpcc-data2/roxie", "/var/lib/HPCCSystems/hpcc-data3/roxie", "/var/lib/HPCCSystems/hpcc-data4/roxie" },
        { "/var/lib/HPCCSystems/hpcc-data/eclagent", "/var/lib/HPCCSystems/hpcc-mirror/eclagent" },
        { "/var/lib/HPCCSystems/hpcc-data/unknown", "/var/lib/HPCCSystems/hpcc-mirror/unknown" },
    };
static const char *componentNames[__grp_size] =
    {
        "thor", "thor", "roxie", "eclagent", "unknown"
    };
static const char *dirTypeNames[MAX_REPLICATION_LEVELS] =
    {
        "data", "data2", "data3", "data4"
    };

static StringAttr windowsBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS];
static StringAttr unixBaseDirectories[__grp_size][MAX_REPLICATION_LEVELS];

static StringAttr defaultpartmask("$L$._$P$_of_$N$");

static SpinLock ldbSpin;
static bool ldbDone = false;
void loadDefaultBases()
{
    SpinBlock b(ldbSpin);
    if (ldbDone)
        return;
    ldbDone = true;

    SessionId mysessid = myProcessSession();
    if (mysessid)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software/Directories", mysessid, RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (conn) {
            IPropertyTree* dirs = conn->queryRoot();
            for (unsigned groupType = 0; groupType < __grp_size; groupType++)
            {
                const char *component = componentNames[groupType];
                for (unsigned replicationLevel = 0; replicationLevel < MAX_REPLICATION_LEVELS; replicationLevel++)
                {
                    StringBuffer dirout;
                    const char *dirType = dirTypeNames[replicationLevel];
                    if (replicationLevel==1 && groupType!=grp_roxie)
                        dirType = "mirror";
                    if (getConfigurationDirectory(dirs, dirType, component,
                        "dummy",   // NB this is dummy value (but actually hopefully not used anyway)
                        dirout))
                       unixBaseDirectories[groupType][replicationLevel].set(dirout.str());
                }
            }
        }
    }
    for (unsigned groupType = 0; groupType < __grp_size; groupType++)
        for (unsigned replicationLevel = 0; replicationLevel < MAX_REPLICATION_LEVELS; replicationLevel++)
        {
            if (unixBaseDirectories[groupType][replicationLevel].isEmpty())
                unixBaseDirectories[groupType][replicationLevel].set(defaultUnixBaseDirectories[groupType][replicationLevel]);
            if (windowsBaseDirectories[groupType][replicationLevel].isEmpty())
                windowsBaseDirectories[groupType][replicationLevel].set(defaultWindowsBaseDirectories[groupType][replicationLevel]);
        }
}


const char *queryBaseDirectory(GroupType groupType, unsigned replicateLevel, DFD_OS os)
{
    if (os==DFD_OSdefault)
#ifdef _WIN32
        os = DFD_OSwindows;
#else
        os = DFD_OSunix;
#endif
    assertex(replicateLevel < MAX_REPLICATION_LEVELS);
    loadDefaultBases();
    switch (os)
    {
    case DFD_OSwindows:
        return windowsBaseDirectories[groupType][replicateLevel];
    case DFD_OSunix:
        return unixBaseDirectories[groupType][replicateLevel];
    }
    return NULL;
}

void setBaseDirectory(const char * dir, unsigned replicateLevel, DFD_OS os)
{
    // 2 possibilities
    // either its an absolute path
    // or use /c$/thordata and /d$/thordata 
    if (os==DFD_OSdefault)
#ifdef _WIN32
        os = DFD_OSwindows;
#else
        os = DFD_OSunix;
#endif
    assertex(replicateLevel < MAX_REPLICATION_LEVELS);
    loadDefaultBases();
    StringBuffer out;
    if (!dir||!*dir||!isAbsolutePath(dir))
        throw MakeStringException(-1,"setBaseDirectory(%s) requires an absolute path",dir ? dir : "null");
    size32_t l = strlen(dir);
    if ((l>3)&&(isPathSepChar(dir[l-1])))
        l--;
    switch (os) {
    case DFD_OSwindows:
        windowsBaseDirectories[grp_unknown][replicateLevel].set(dir,l);
        break;
    case DFD_OSunix:
        unixBaseDirectories[grp_unknown][replicateLevel].set(dir,l);
        break;
    }
}


const char *queryPartMask()
{
    return defaultpartmask.get();
}

void setPartMask(const char * mask)
{
    defaultpartmask.set(mask);
}

StringBuffer &getPartMask(StringBuffer &ret,const char *lname,unsigned partmax)
{
    // ret is in *and* out
    StringAttr tmp;
    const char *m;
    if (!ret.length())
        m = queryPartMask();
    else {
        tmp.set(ret.str());
        m = tmp.get();
        ret.clear();
    }
    StringBuffer lns;
    if (lname) {
        bool maybequery = false;
        const char *lnamebase = lname;
        loop {
            const char *e = strstr(lname,"::");
            if (!e)
                break;
            lname = e+2;
            if (*lname=='>')
                maybequery = true;
        }
        if (maybequery) {
            CDfsLogicalFileName lfn;
            lfn.set(lnamebase);
            if (lfn.isQuery()) {
                RemoteFilename rfn;
                lfn.getExternalFilename(rfn);
                StringBuffer path;
                rfn.getPath(path);
                // start at third separator
                const char *s = path.str();
                unsigned si = 0;
                while (*s&&(si!=3)) {
                    if (isPathSepChar(*s))
                        si++;
                    s++;
                }
                return ret.append(s);
            }
        }
        char c;
        const char *l = lname;
        while ((c=*(l++))!=0) {
            if (validFNameChar(c))
                lns.append(c);
            else
                lns.appendf("%%%.2X", (int) c);
        }
        lns.trim().toLowerCase();
    }
    else if (!partmax)
        return ret.append(m);
    char c;
    while ((c=*(m++))!=0) {
        if (c=='$') {
            char pc = toupper(m[0]);
            if (pc&&(m[1]=='$')) {
                switch (pc) {
                case 'L':
                    if (lname) {
                        ret.append(lns.str());
                        m+=2;
                        continue;
                    }
                case 'N':
                    if (partmax) {
                        ret.append(partmax);
                        m+=2;
                        continue;
                    }
                }
            }
        }
        ret.append(c);
    }
    return ret;
}

inline const char *skipRoot(const char *lname)
{
    loop {
        while (*lname==' ')
            lname++;
        if (*lname!='.')
            break;
        const char *s = lname+1;
        while (*s==' ')
            s++;
        if (!*s)
            lname = s;
        else if ((s[0]==':')&&(s[1]==':'))
            lname = s+2;
        else
            break;
    }
    return lname;
}



StringBuffer &makePhysicalPartName(const char *lname, unsigned partno, unsigned partmax, StringBuffer &result, unsigned replicateLevel, DFD_OS os,const char *diroverride)
{
    assertex(lname);
    if (strstr(lname,"::>")) { // probably query
        CDfsLogicalFileName lfn;
        lfn.set(lname);
        if (lfn.isQuery()) {
            RemoteFilename rfn;
            lfn.getExternalFilename(rfn);
            StringBuffer path;
            rfn.getPath(path);
            // query start at third separator
            const char *s = path.str();
            const char *sb = s;
            unsigned si = 0;
            while (*s&&(si!=3)) {
                if (isPathSepChar(*s)) {
                    if (os!=DFD_OSdefault)
                        path.setCharAt(s-sb,OsSepChar(os));
                    si++;
                }
                s++;
            }
            if (partno==0)
                return result.append(s-sb,sb);
            return result.append(sb);
        }
    }

    if (diroverride&&*diroverride) {
        if (os==DFD_OSdefault)
            os = SepCharBaseOs(getPathSepChar(diroverride));
        result.append(diroverride);
    }
    else
        result.append(queryBaseDirectory(grp_unknown, replicateLevel, os));

    size32_t l = result.length();
    if ((l>3)&&(result.charAt(l-1)!=OsSepChar(os))) {
        result.append(OsSepChar(os));
        l++;
    }

    lname = skipRoot(lname);
    char c;
    while ((c=*(lname++))!=0) {
        if ((c==':')&&(*lname==':')) {
            lname++;
            result.clip().append(OsSepChar(os));
            l = result.length();
            lname = skipRoot(lname);
        }
        else if (validFNameChar(c))
            result.append((char)tolower(c));
        else
            result.appendf("%%%.2X", (int) c);
    }
    if (partno==0) { // just return directory (with trailing PATHSEP)
        result.setLength(l);
    }
    else {
#ifndef INCLUDE_1_OF_1
        if (partmax>1)  // avoid 1_of_1
#endif
        {
            StringBuffer tail(result.str()+l);
            tail.trim();
            result.setLength(l);
            const char *m = queryPartMask();
            while ((c=*(m++))!=0) {
                if (c=='$') {
                    char pc = toupper(m[0]);
                    if (pc&&(m[1]=='$')) {
                        switch (pc) {
                        case 'P':
                            result.append(partno);
                            m+=2;
                            continue;
                        case 'N':
                            result.append(partmax);
                            m+=2;
                            continue;
                        case 'L':
                            result.append(tail);
                            m+=2;
                            continue;
                        }
                    }
                }
                result.append(c);
            }
        }
    }
    return result.clip();
}

StringBuffer &makeSinglePhysicalPartName(const char *lname, StringBuffer &result, bool allowospath, bool &wasdfs,const char *diroverride)
{
    wasdfs = !(allowospath&&(isAbsolutePath(lname)||(stdIoHandle(lname)>=0)));
    if (wasdfs)
        return makePhysicalPartName(lname, 1, 1, result, false, DFD_OSdefault,diroverride);
    return result.append(lname);
}

bool setReplicateDir(const char *dir,StringBuffer &out,bool isrep,const char *baseDir,const char *repDir)
{
    // assumes that dir contains a separator (like base)
    if (!dir)
        return false;
    const char *sep=findPathSepChar(dir);
    if (!sep)
        return false;
    DFD_OS os = SepCharBaseOs(*sep);
    const char *d = baseDir?baseDir:queryBaseDirectory(grp_unknown, isrep ? 0 : 1,os);
    if (!d)
        return false;
    unsigned match = 0;
    unsigned count = 0;
    unsigned i;
    for (i=0;d[i]&&dir[i]&&(d[i]==dir[i]);i++)
        if (isPathSepChar(dir[i])) {
            match = i;
            count++;
        }
    const char *r = repDir?repDir:queryBaseDirectory(grp_unknown, isrep ? 1 : 0,os);
    if (d[i]==0) {
        if ((dir[i]==0)||isPathSepChar(dir[i])) {
            out.append(r).append(dir+i);
            return true;
        }
    }
    else if (count) { // this is a bit of a kludge to handle roxie backup
        const char *s = r;
        const char *b = s;
        while (s&&*s) {
            if (isPathSepChar(*s)) {
                if (--count==0) {
                    out.append(s-b,b).append(dir+match);
                    return true;
                }
            }
            s++;
        }
    }
    return false;
}


IFileDescriptor *createMultiCopyFileDescriptor(IFileDescriptor *in,unsigned num)
{
    Owned<IFileDescriptor> out = createFileDescriptor(createPTreeFromIPT(&in->queryProperties()));
    IPropertyTree &t = out->queryProperties();
    __int64 rc = t.getPropInt64("@recordCount",-1);
    if (rc>0)
        t.setPropInt64("@recordCount",rc*num);
    __int64 sz = t.getPropInt64("@size",-1);
    if (sz>0)
        t.setPropInt64("@size",sz*num);
    Owned<IPartDescriptorIterator> iter=in->getIterator();
    unsigned n = 0;
    while (num--) {
        if (iter->first()) {
            do {
                IPartDescriptor &part = iter->query();
                RemoteFilename rfn;
                part.getFilename(0,rfn);
                out->setPart(n,rfn,&part.queryProperties());
                n++;
            } while (iter->next());
        }
    }
    return out.getClear();
}

void removePartFiles(IFileDescriptor *desc,IMultiException *mexcept)
{
    if (!desc)
        return;
    CriticalSection crit;
    class casyncfor: public CAsyncFor
    {
        CriticalSection &crit;
        IMultiException *mexcept;
        IFileDescriptor *parent;
    public:
        casyncfor(IFileDescriptor *_parent,IMultiException *_mexcept,CriticalSection &_crit)
            : crit(_crit)
        {
            parent = _parent;
            mexcept = _mexcept;
        }
        void Do(unsigned i)
        {
            CriticalBlock block(crit);
            unsigned nc = parent->numCopies(i);
            for (unsigned copy = 0; copy < nc; copy++) {
                RemoteFilename rfn;
                parent->getFilename(i,copy,rfn);
                Owned<IFile> partfile = createIFile(rfn);
                StringBuffer eps;
                try
                {
                    unsigned start = msTick();
                    CriticalUnblock unblock(crit);
                    if (partfile->remove()) {
//                          PROGLOG("Removed '%s'",partfile->queryFilename());
                        unsigned t = msTick()-start;
                        if (t>60*1000)
                            LOG(MCwarning, unknownJob, "Removing %s from %s took %ds", partfile->queryFilename(), rfn.queryEndpoint().getUrlStr(eps).str(), t/1000);
                    }
//                      else
//                          LOG(MCwarning, unknownJob, "Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                }
                catch (IException *e)
                {
                    if (mexcept)
                        mexcept->append(*e);
                    else {
                        StringBuffer s("Failed to remove file part ");
                        s.append(partfile->queryFilename()).append(" from ");
                        rfn.queryEndpoint().getUrlStr(s);
                        EXCLOG(e, s.str());
                        e->Release();
                    }
                }
            }
        }
    } afor(desc,mexcept,crit);
    afor.For(desc->numParts(),10,false,true);
}

StringBuffer &setReplicateFilename(StringBuffer &filename,unsigned drvnum,const char *baseDir,const char *repDir)
{
    if (!drvnum)
        return filename;  //do nothing!
    StringBuffer tmp(filename); // bit klunky
    if (strcmp(swapPathDrive(tmp,0,drvnum).str(),filename.str())!=0)
        tmp.swapWith(filename);
    else if (drvnum==1) { // OSS
        if(setReplicateDir(filename.str(),tmp.clear(),true,baseDir,repDir))
            tmp.swapWith(filename);
    }
    return filename;
}

IGroup *shrinkRepeatedGroup(IGroup *grp)
{
    if (!grp)
        return NULL;
    unsigned w = grp->ordinality();
    for (unsigned i=1;i<w;i++) {
        unsigned j;
        for (j=i;j<w;j++) 
            if (!grp->queryNode(j).equals(&grp->queryNode(j%i)))
                break;
        if (j==w)
            return grp->subset(0U,i);
    }
    return LINK(grp);

}


IFileDescriptor *createFileDescriptorFromRoxieXML(IPropertyTree *tree,const char *clustername)
{
    if (!tree)
        return NULL;
    bool iskey = (strcmp(tree->queryName(),"Key")==0);
    Owned<IPropertyTree> attr = createPTree("Attr");
    Owned<IFileDescriptor> res = createFileDescriptor(attr.getLink());
    const char *id = tree->queryProp("@id");
    if (id) {
        if (*id=='~')
            id++;
        res->setTraceName(id);
    }
    else 
        id = "";
    const char *dir = tree->queryProp("@directory");
    if (!dir||!*dir)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing directory",id); 
    const char *mask = tree->queryProp("@partmask");
    if (!mask||!*mask)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part mask",id); 
    unsigned np = tree->getPropInt("@numparts");
    IPropertyTree *part1 = tree->queryPropTree("Part_1");
    if (!part1)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part 1",id); 
    
    // assume same number of copies for all parts
    unsigned nc = 0;
    StringBuffer xpath;
    StringBuffer locpath;
    StringArray locdirs;
    loop {
        IPropertyTree *loc =  part1->queryPropTree(xpath.clear().appendf("Loc[%d]",nc+1));
        if (!loc)
            break;
        const char *path = loc->queryProp("@path");
        if (!path)
            throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part 1 loc path",id); 
        RemoteFilename rfn;
        rfn.setRemotePath(path);
        if (rfn.queryEndpoint().isNull()) 
            break;
        locdirs.append(rfn.getLocalPath(locpath.clear()).str());
        nc++;
    }
    if (!nc)
        throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part 1 Loc",id);
    StringBuffer fulldir(locdirs.item(0));
    addPathSepChar(fulldir).append(tree->queryProp("@directory"));
    res->setDefaultDir(fulldir.str());
    // create a group
    SocketEndpointArray *epa = new SocketEndpointArray[nc];
    for (unsigned p=1;p<=np;p++) {
        IPropertyTree *part = tree->queryPropTree(xpath.clear().appendf("Part_%d",p));
        if (!part)
            throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part %d",id,p); 
        if (iskey&&(p==np)&&(np>1)) // leave off tlk
            continue;
        unsigned c;
        for(c = 0;c<nc;c++) {
            IPropertyTree *loc = part->queryPropTree(xpath.clear().appendf("Loc[%d]",c+1));
            if (loc) {
                const char *path = loc->queryProp("@path");
                if (!path)
                    throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part %d loc path",id,p); 
                RemoteFilename rfn;
                rfn.setRemotePath(path);
                bool found = false;
                ForEachItemIn(d,locdirs) {
                    if (strcmp(rfn.getLocalPath(locpath.clear()).str(),locdirs.item(d))==0) {
                        SocketEndpoint ep = rfn.queryEndpoint();
                        if (ep.port==DAFILESRV_PORT || ep.port==SECURE_DAFILESRV_PORT)
                            ep.port = 0;
                        epa[d].append(ep);
                        found = true;
                        break;
                    }
                }
            }
            else
                ERRLOG("createFileDescriptorFromRoxie: %s missing part %s",id,xpath.str()); 
        }
    }
    res->setPartMask(mask);
    res->setNumParts(np);
    SocketEndpointArray merged; // this is a bit odd but needed for when num parts smaller than cluster width
    ForEachItemIn(ei1,epa[0])
        merged.append(epa[0].item(ei1));
    for (unsigned enc=1;enc<nc;enc++) { // not quick! (n^2)
        ForEachItemIn(ei2,epa[enc]) {
            SocketEndpoint ep = epa[enc].item(ei2);
            ForEachItemIn(ei3,merged) {
                if (!merged.item(ei3).equals(ep)) {
                    merged.append(ep);
                    break;
                }
            }
        }
    }
    Owned<IGroup> epgrp = createIGroup(merged);
    Owned<IGroup> grp = shrinkRepeatedGroup(epgrp);
    // find replication offset
    ClusterPartDiskMapSpec map;
    if (nc) {
        map.replicateOffset = 0;
        unsigned i2;
        unsigned i3;
        loop {
            for (i2=1;i2<nc;i2++) {
                for (i3=0;i3<epa[i2].ordinality();i3++) {
                    INode &node = grp->queryNode((i3+map.replicateOffset*i2)%grp->ordinality());
                    if (!node.endpoint().equals(epa[i2].item(i3)))
                        break;
                }
                if (i3<epa[i2].ordinality())
                    break;
            }
            if (i2==nc)
                break;
            map.replicateOffset++;
            if (map.replicateOffset==grp->ordinality())
                throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s cannot determine replication offset",id);
        }
    }

    map.defaultCopies = nc;
    if (iskey) {
        map.repeatedPart = (np-1);
        map.flags |= CPDMSF_repeatedPart;
    }
    if (clustername) {
#if 0
        Owned<IGroup> cgrp = queryNamedGroupStore().lookup(clustername);
        if (!cgrp) 
            throw MakeStringException(-1,"createFileDescriptorFromRoxieXML: Cluster %s not found",clustername);
        if (!cgrp->equals(grp))
            throw MakeStringException(-1,"createFileDescriptorFromRoxieXML: Cluster %s does not match XML",clustername);
#endif
        res->addCluster(clustername,grp,map);
    }
    else
        res->addCluster(grp,map);
    delete [] epa;
    for (unsigned p=1;;p++) {
        IPropertyTree *part = tree->queryPropTree(xpath.clear().appendf("Part_%d",p));
        if (!part)
            break;
        IPropertyTree &pprop = res->queryPart(p-1)->queryProperties();
        StringBuffer ps;
        if (part->getProp("@crc",ps.clear())&&ps.length())
            pprop.setProp("@fileCRC",ps.str());
        if (part->getProp("@size",ps.clear())&&ps.length())
            pprop.setProp("@size",ps.str());
        if (part->getProp("@modified",ps.clear())&&ps.length())
            pprop.setProp("@modified",ps.str());
        if (iskey&&(p==np-1))
            pprop.setProp("@kind","topLevelKey");

#ifdef _DEBUG // test parts match
        unsigned c;
        for(c = 0;c<nc;c++) {
            IPropertyTree *loc = part->queryPropTree(xpath.clear().appendf("Loc[%d]",c+1));
            if (loc) {
                const char *path = loc->queryProp("@path");
                if (!path)
                    throw MakeStringException(-1,"createFileDescriptorFromRoxie: %s missing part %d loc path",id,c+1); 
                StringBuffer fullpath(path);
                addPathSepChar(fullpath).append(tree->queryProp("@directory"));
                expandMask(addPathSepChar(fullpath),mask,p-1,np);
                RemoteFilename rfn;
                rfn.setRemotePath(fullpath.str());
                if (!rfn.queryEndpoint().isNull()) {
                    unsigned c2;
                    for (c2=0;c2<res->numCopies(p-1);c2++) {
                        RemoteFilename rfn2;
                        res->getFilename(p-1,c2,rfn2);
                        if (rfn2.equals(rfn))
                            break;
                        StringBuffer tmp;
                        rfn2.getPath(tmp);
                        //PROGLOG("%s",tmp.str());
                    }
                    if (c2==res->numCopies(p-1)) {
                        res->numCopies(p-1);
                        PROGLOG("ERROR: createFileDescriptorFromRoxie [%d,%d] %s not found",p,c,fullpath.str());
                    }
                }
            }
        }
#endif
    }
    IPropertyTree &fprop = res->queryProperties();
    StringBuffer fps;
    if (tree->getProp("@crc",fps.clear())&&fps.length())
        fprop.setProp("@checkSum",fps.str());
    if (tree->getProp("@recordCount",fps.clear())&&fps.length())
        fprop.setProp("@recordCount",fps.str());
    if (tree->getProp("@size",fps.clear())&&fps.length())
        fprop.setProp("@size",fps.str());
    if (tree->getProp("@formatCrc",fps.clear())&&fps.length())
        fprop.setProp("@formatCrc",fps.str());
    MemoryBuffer mb;
    if (tree->getPropBin("_record_layout", mb))
        fprop.setPropBin("_record_layout", mb.length(), mb.toByteArray());
    if (iskey) {
        fprop.setProp("@kind","key");
    }
    return res.getLink();
}
