/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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



#ifndef JFILE_HPP
#define JFILE_HPP

#include "jiface.hpp"
#include "jio.hpp"
#include "jtime.hpp"
#include "jsocket.hpp"


interface IFile;
interface IFileIO;
interface IFileAsyncIO;
interface IFileAsyncResult;
interface IFileIOStream;
interface IMemoryMappedFile;

class MemoryBuffer;
class Semaphore;

enum IFOmode { IFOcreate, IFOread, IFOwrite, IFOreadwrite, IFOcreaterw };    // modes for open
enum IFSHmode { IFSHnone, IFSHread=0x8, IFSHfull=0x10};   // sharing modes
enum IFSmode { IFScurrent = FILE_CURRENT, IFSend = FILE_END, IFSbegin = FILE_BEGIN };    // seek mode
enum CFPmode { CFPcontinue, CFPcancel, CFPstop };    // modes for ICopyFileProgress::onProgress return

class CDateTime;

interface IDirectoryIterator : extends IIteratorOf<IFile> 
{
    virtual StringBuffer &getName(StringBuffer &buf)=0;
    virtual bool isDir()=0;
    virtual __int64 getFileSize()=0;
    virtual bool getModifiedTime(CDateTime &ret)=0;

};


#define IDDIunchanged   1
#define IDDImodified    2
#define IDDIadded       4
#define IDDIdeleted     8
#define IDDIstandard    (IDDIunchanged|IDDImodified|IDDIadded)
#define IDDIchanged     (IDDImodified|IDDIadded|IDDIdeleted)

interface IDirectoryDifferenceIterator : extends IDirectoryIterator
{
    virtual void setMask(unsigned mask)=0;      // called before first (combination of IDDI*)
    virtual unsigned getFlags()=0;              // called on each iteration returns IDDI*
};


interface ICopyFileProgress
{
    virtual CFPmode onProgress(offset_t sizeDone, offset_t totalSize) = 0; 
};

class RemoteFilename;

enum fileBool { foundNo = false, foundYes = true, notFound = 2 };
interface IFile :extends IInterface
{
    virtual bool exists() = 0; // NB this can raise exceptions if the machine doesn't exist or other fault
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime) = 0;
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime) = 0;
    virtual fileBool isDirectory() = 0;
    virtual fileBool isFile() = 0;
    virtual fileBool isReadOnly() = 0;
    virtual IFileIO * open(IFOmode mode) = 0;
    virtual IFileAsyncIO * openAsync(IFOmode mode) = 0;
    virtual IFileIO * openShared(IFOmode mode,IFSHmode shmode) = 0;
    virtual const char * queryFilename() = 0;
    virtual bool remove() = 0;
    virtual void rename(const char *newTail) = 0;       // tail only preferred but can have full path if exactly matches existing dir
    virtual void move(const char *newName) = 0;         // can move between directories on same node (NB currently not always supported on remote files!)
    virtual void setReadOnly(bool ro) = 0;
    virtual offset_t size() = 0;
    virtual bool setCompression(bool set) = 0;
    virtual offset_t compressedSize() = 0;
    virtual unsigned getCRC() = 0;
    virtual void setCreateFlags(unsigned cflags) =0;    // I_S*
    virtual void setShareMode(IFSHmode shmode) =0;

// Directory functions
    virtual bool createDirectory() = 0;
    virtual IDirectoryIterator *directoryFiles(const char *mask=NULL,bool sub=false,bool includedirs=false)=0;
    virtual IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL)=0; // returns NULL if timed out or abortsem signalled
    virtual bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime) = 0; // return false if doesn't exist
                                                                            // size is undefined if directory
    virtual void copySection(const RemoteFilename &dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress *progress=NULL) =0;
    // if toOfs is (offset_t)-1 then copies entire file 

    virtual void copyTo(IFile *dest, size32_t buffersize=0x100000, ICopyFileProgress *progress=NULL, bool usetmp=false)=0;

    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false)=0;

    virtual void treeCopyTo(IFile *dest,IpSubNet &subnet,IpAddress &resfrom,bool usetmp=false) = 0;


};

struct CDirectoryEntry: public CInterface
{ // for cloning IDirectoryIterator iterator
public:
    IMPLEMENT_IINTERFACE;
    CDirectoryEntry() {}
    CDirectoryEntry(IDirectoryIterator &iter)
        : file(&iter.query()), isdir(iter.isDir()), size(iter.getFileSize())
    {
        StringBuffer tmp;
        name.set(iter.getName(tmp));
        iter.getModifiedTime(modifiedTime);
    }
    Linked<IFile> file;
    StringAttr name;
    bool isdir;
    __int64 size;
    CDateTime modifiedTime;
};

typedef enum { SD_nosort, SD_byname, SD_bynameNC, SD_bydate, SD_bysize }  SortDirectoryMode;


extern jlib_decl unsigned sortDirectory( 
                        CIArrayOf<CDirectoryEntry> &sortedfiles, // returns sorted directory
                        IDirectoryIterator &iter, 
                        SortDirectoryMode mode,
                        bool rev=false,                             // reverse sort
                        bool includedirs=false
                      );



//This is closed by releasing the interface
interface IFileIO : public IInterface
{
    virtual size32_t read(offset_t pos, size32_t len, void * data) = 0;
    virtual offset_t size() = 0;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) = 0;
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) =0;
    virtual void setSize(offset_t size) = 0;
    virtual void flush() = 0;
    virtual void close() = 0;       // no other access is allowed after this call
};

interface IFileIOCache : extends IInterface
{
    virtual IFileIO *addFile( RemoteFilename &filename, IFOmode mode ) = 0;
};

interface IMemoryMappedFile: extends IInterface
{
    virtual byte *base()=0;                 // address of currently mapped section
    virtual offset_t offset()=0;            // offset in file of currently mapped section
    virtual memsize_t length()=0;           // size of currently mapped section
    virtual offset_t fileSize()=0;          // size of total file
    virtual int compareWithin(const void *p)=0;   // return 0 if pointer within mapped section -1 if lt +1 if gt
    virtual bool writeAccess()=0;           // true if write enabled map
    virtual void flush()=0;                 // flushed written buffers 
    virtual byte *nextPtr(const void *ptr,offset_t skip, memsize_t extent, memsize_t &got)=0; // used to move about in partially mapped file
    virtual void reinit(offset_t ofs, memsize_t len=(memsize_t)-1, bool write=false)=0; // move map
};


interface IFileAsyncResult: extends IInterface
{
    virtual bool getResult(size32_t &result,bool wait) =0 ; // returns false if wait false and not finished
};

interface IFileAsyncIO : extends IFileIO
{
    virtual IFileAsyncResult *readAsync(offset_t pos, size32_t len, void * data) = 0; // data must be available until getResult returns true
    virtual IFileAsyncResult *writeAsync(offset_t pos, size32_t len, const void * data) = 0; // data must be available until getResult returns true
};


interface IFileIOStream : extends IIOStream
{
    virtual void seek(offset_t pos, IFSmode origin) = 0;
    virtual offset_t size() = 0;
    virtual offset_t tell() = 0;
};

interface IDiscretionaryLock: extends IInterface
{
    virtual bool lock(bool exclusive=true, unsigned timeout=INFINITE) = 0; // overrides previous setting
    virtual void unlock() = 0;
    virtual bool isLocked() = 0;
    virtual bool isExclusiveLocked() = 0;
};

//-- Interfaces/functions used for managing passwords needed to access remote machines.

class IpAddress;
interface IPasswordProvider : public IInterface
{
    virtual bool getPassword(const IpAddress & ip, StringBuffer & username, StringBuffer & password) = 0;

};


#ifndef _WIN32
#define _MAX_DRIVE      4
#define _MAX_DIR        256
#define _MAX_FNAME      256
#define _MAX_EXT        256
void jlib_decl _splitpath(const char *path, char *drive, char *dir, char *fname, char *ext);
#endif

extern jlib_decl void setDefaultUser(const char * username,const char *password);
extern jlib_decl IPasswordProvider * queryPasswordProvider();
extern jlib_decl void setPasswordProvider(IPasswordProvider * provider);

//-- Helper routines 


extern jlib_decl size32_t read(IFileIO * in, offset_t pos, size32_t len, MemoryBuffer & buffer);
extern jlib_decl void copyFile(const char *target, const char *source, size32_t buffersize=0x100000, ICopyFileProgress *progress=NULL);
extern jlib_decl void copyFile(IFile * target, IFile * source,size32_t buffersize=0x100000, ICopyFileProgress *progress=NULL);
extern jlib_decl bool recursiveCreateDirectory(const char * path);              // only works locally, use IFile::createDirectory() for remote
extern jlib_decl bool recursiveCreateDirectoryForFile(const char *filename);    // only works locally, use IFile::createDirectory() for remote

extern jlib_decl void splitFilename(const char * filename, StringBuffer * drive, StringBuffer * path, StringBuffer * tail, StringBuffer * ext, bool longExt = false);
extern jlib_decl bool splitUNCFilename(const char * filename, StringBuffer * machine, StringBuffer * path, StringBuffer * tail, StringBuffer * ext);
extern jlib_decl StringBuffer& createUNCFilename(const char * localfilename, StringBuffer &UNC, bool useHostNames=true);
extern jlib_decl bool ensureFileExtension(StringBuffer& filename, const char* desiredExtension);
extern jlib_decl StringBuffer& getFullFileName(StringBuffer& filename, bool noExtension = false);
extern jlib_decl StringBuffer& getFileNameOnly(StringBuffer& filename, bool noExtension = true);
extern jlib_decl offset_t filesize(const char *fname);
extern jlib_decl offset_t getFreeSpace(const char* name);
extern jlib_decl void createHardLink(const char* fileName, const char* existingFileName);

//-- Creation routines for implementations of the interfaces above

extern jlib_decl IFile * createIFile(const char * filename);
extern jlib_decl IFile * createIFile(MemoryBuffer & buffer);
extern jlib_decl IFileIO * createIFileIO(HANDLE handle);
extern jlib_decl IDirectoryIterator * createDirectoryIterator(const char * path = NULL, const char * wildcard = NULL);
extern jlib_decl IDirectoryIterator * createNullDirectoryIterator();
extern jlib_decl IFileIO * createIORange(IFileIO * file, offset_t header, offset_t length);     // restricts input/output to a section of a file.

extern jlib_decl IFileIOStream * createIOStream(IFileIO * file);        // links argument
extern jlib_decl IFileIOStream * createBufferedIOStream(IFileIO * file, unsigned bufsize=(unsigned)-1);// links argument
extern jlib_decl IFileIOStream * createBufferedAsyncIOStream(IFileAsyncIO * file, unsigned bufsize=(unsigned)-1);// links argument

// Useful for commoning up file and string based processing
extern jlib_decl IFileIO * createIFileI(unsigned len, const void * buffer);     // input only...
extern jlib_decl IFileIO * createIFileIO(unsigned len, void * buffer);
extern jlib_decl IFileIO * createIFileIO(StringBuffer & buffer);
extern jlib_decl IFileIO * createIFileIO(MemoryBuffer & buffer);

//-- Creation of routines to implement other interfaces on the interfaces above.

interface IReadSeq;
interface IWriteSeq;

// NB the following are unbuffered 
extern jlib_decl IReadSeq *createReadSeq(IFileIOStream * stream, offset_t _offset, size32_t size); // no buffering
extern jlib_decl IWriteSeq *createWriteSeq(IFileIOStream * stream, size32_t size);                 // no buffering


extern jlib_decl IDiscretionaryLock *createDiscretionaryLock(IFile *file);
extern jlib_decl IDiscretionaryLock *createDiscretionaryLock(IFileIO *fileio);




// useful stream based reader 
interface ISerialStream: extends IInterface
{
    virtual const void * peek(size32_t wanted,size32_t &got) = 0;   // try and ensure wanted bytes are available. 
                                                                    // if got<wanted then approaching eof
                                                                    // if got>wanted then got is size available in buffer

    virtual void get(size32_t len, void * ptr) = 0;                 // exception if no data available
    virtual bool eos() = 0;                                         // no more data
    virtual void skip(size32_t sz) = 0;
    virtual offset_t tell() = 0;
    virtual void reset(offset_t _offset,offset_t _flen=(offset_t)-1) = 0;       // input stream has changed - restart reading
};

/* example of reading a nul terminated string using ISerialStream peek and skip
{
    loop {
        const char *s = peek(1,got);
        if (!s)
            break;  // eof before nul detected;
        const char *p = s;
        const char *e = p+got;
        while (p!=e) {
            if (!*p) {
                out.append(p-s,s);
                skip(p-s+1); // include nul
                return;
            }
            p++;
        }
        out.append(got,s);
        skip(got);
    }
}
*/



interface IFileSerialStreamCallback  // used for CRC tallying
{
    virtual void process(offset_t ofs, size32_t sz, const void *buf) = 0;
};


extern jlib_decl ISerialStream *createSimpleSerialStream(ISimpleReadStream * in, size32_t bufsize = (size32_t)-1, IFileSerialStreamCallback *callback=NULL);
extern jlib_decl ISerialStream *createSocketSerialStream(ISocket * in, unsigned timeoutms, size32_t bufsize = (size32_t)-1, IFileSerialStreamCallback *callback=NULL);
extern jlib_decl ISerialStream *createFileSerialStream(IFileIOStream * in, size32_t bufsize = (size32_t)-1, IFileSerialStreamCallback *callback=NULL);
extern jlib_decl ISerialStream *createFileSerialStream(IFileIO *fileio, offset_t ofs=0, offset_t flen=(offset_t)-1,size32_t bufsize = (size32_t)-1, IFileSerialStreamCallback *callback=NULL);
extern jlib_decl ISerialStream *createFileSerialStream(IMemoryMappedFile *mmapfile, offset_t ofs=0, offset_t flen=(offset_t)-1, IFileSerialStreamCallback *callback=NULL);
extern jlib_decl ISerialStream *createMemorySerialStream(const void *buffer, memsize_t len, IFileSerialStreamCallback *callback=NULL);
extern jlib_decl ISerialStream *createMemoryBufferSerialStream(MemoryBuffer & buffer, IFileSerialStreamCallback *callback=NULL);



typedef Linked<IFile> IFileAttr;
typedef Linked<IFileIO> IFileIOAttr;
typedef Linked<IFileIOStream> IFileIOStreamAttr;
typedef Owned<IFile> OwnedIFile;
typedef Owned<IFileIO> OwnedIFileIO;
typedef Owned<IFileIOStream> OwnedIFileIOStream;

//-- RemoteFilename class (file location encapsulation)

class jlib_decl RemoteFilename
{
    void badFilename(const char * filename);

    SocketEndpoint      ep;         // node for file (port is used for daliservix and user defined ports)
    StringAttr          localhead;  // local base directory 
    StringAttr          sharehead;  // remote share equvalent to localbase (always starts with separator if present)
    StringAttr          tailpath;   // tail directory and filename appended to one of the above (always starts with separator)
public:
    void clear();

    StringBuffer & getTail(StringBuffer &name) const;       // Tail Name (e.g. "test.d00._1_of_3")
    StringBuffer & getPath(StringBuffer & name) const;      // Either local or full depending on location 
    StringBuffer & getLocalPath(StringBuffer &name) const;  // Local Path (e.g. "c:\dfsdata\test.d00._1_of_3")
    StringBuffer & getRemotePath(StringBuffer &name) const; // Full Remote Path  (e.g. "\\192.168.0.123\c$\dfsdata\test.d00._1_of_3")
    
    bool isLocal() const;                                   // on calling node
    bool isUnixPath() const;                                // a unix filename
    char getPathSeparator() const;                          // separator for this path
    const SocketEndpoint & queryEndpoint() const            { return ep; } // node containing file
    const IpAddress      & queryIP() const                  { return ep; }
    unsigned short getPort() const                          { return ep.port; }
    bool isNull() const;                        

    // the following overwrite previous contents
    void set(const RemoteFilename & other);         
    void setPath(const SocketEndpoint & _ep, const char * filename); // filename should be full windows or unix path (local)
    void setLocalPath(const char *name);                    // local path - can partial but on linux must be under \c$ or \d$
    void setRemotePath(const char * url,const char *local=NULL); // url should be full (share) path including ep

    // the following modify existing 
    void setIp(const IpAddress & ip)                        { ep.ipset(ip); }
    void setEp(const SocketEndpoint & _ep)                  { ep.set(_ep); }
    void setPort(unsigned short port)                       { ep.port=port; }
    void setShareHead(const char *name)                     { sharehead.set(name); }
    void setLocalHead(const char *name)                     { localhead.set(name); }
    void setTailPath(const char *name)                      { tailpath.set(name); }
    void setExtension(const char * newext);

    void split(StringBuffer * drive, StringBuffer * path, StringBuffer * tail, StringBuffer * ext) const;   
        // same buffer can be passed to several
        // note that this is for a *local* file


    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);
    bool equals(const RemoteFilename & other) const;
};

inline RemoteFilename &Array__Member2Param(RemoteFilename &src)         { return src; }
inline void Array__Assign(RemoteFilename & dest, RemoteFilename &src)   { RemoteFilename tmp; memcpy(&dest,&tmp,sizeof(dest)); dest.set(src); }
inline bool Array__Equal(RemoteFilename &m, RemoteFilename &p)          { return m.equals(p); }
inline void Array__Destroy(RemoteFilename &p)                           { p.clear(); }
class RemoteFilenameArray : public ArrayOf<RemoteFilename, RemoteFilename &> { };

class jlib_decl RemoteMultiFilename: public RemoteFilenameArray
{   // NB all entries must be on on the same node
    SocketEndpoint ep;
    Int64Array sizescache;
public:
    static void expand(const char *mpath, StringArray &array);
    static void tostr(StringArray &array,StringBuffer &out);
    void append(const char *path,const char *defaultdir=NULL);      
                                        // can be local or remote URL path (though urls must point at same machine)
                                        // can contain comma separated entries including wildcards
                                        // if no directory then uses defaultdir if present
    void append(const RemoteFilename &filename);
    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);
    bool isWild(unsigned idx=(unsigned)-1) const;   // true if component wild (if -1 then if *any* component wild)
    void expandWild();

    const SocketEndpoint & queryEndpoint() const            { return ep; } // node containing file
    const IpAddress      & queryIP() const                  { return ep; }
    unsigned short getPort() const                          { return ep.port; }
    void setIp(const IpAddress & ip);
    void setEp(const SocketEndpoint & _ep);
    void setPort(unsigned short port);
    void clear()                                            { ep.set(NULL,0); RemoteFilenameArray::kill(); sizescache.kill(); }
    void set(const RemoteMultiFilename & other);
    bool equals(const RemoteMultiFilename & other);
    offset_t getSize(unsigned i);                           // returns file size (optimizes if loaded via wild card)
};


interface IReplicatedFile: extends IInterface
{
    virtual RemoteFilenameArray &queryCopies()=0;
    virtual IFile *open()=0;
};


extern jlib_decl IReplicatedFile *createReplicatedFile();


interface IRemoteFileCreateHook: extends IInterface
{
    virtual IFile * createIFile(const RemoteFilename & filename)=0;
};
extern jlib_decl void addIFileCreateHook(IRemoteFileCreateHook *);
extern jlib_decl void removeIFileCreateHook(IRemoteFileCreateHook *);

extern jlib_decl IFile * createIFile(const RemoteFilename & filename);

// Hook mechanism for accessing files inside containers (eg zipfiles)

interface IContainedFileHook: extends IInterface
{
    virtual IFile * createIFile(const char *fileName) = 0;
};
extern jlib_decl void addContainedFileHook(IContainedFileHook *);
extern jlib_decl void removeContainedFileHook(IContainedFileHook *);

// Useful set of path inlines that work with '/' and '\'

inline bool isPathSepChar(char sep)
{
    return (sep=='\\')||(sep=='/');     
}

inline const char *findPathSepChar(const char *s)
{
    if (s) while (*s) {
        if (isPathSepChar(*s))
            return s;
        s++;
    }
    return NULL;
}

inline char getPathSepChar(const char *dir)
{
    const char *s=findPathSepChar(dir);
    return s?(*s):((*dir&&(dir[1]==':'))?'\\':PATHSEPCHAR);
}

inline bool containsPathSepChar(const char *s)
{
    return findPathSepChar(s)!=NULL;    
}

inline StringBuffer &addPathSepChar(StringBuffer &path,char sepchar=0)
{
    if (!path.length() || !isPathSepChar(path.charAt(path.length()-1)))
        path.append(sepchar?sepchar:getPathSepChar(path.str()));
    return path;
}

inline StringBuffer &removeTrailingPathSepChar(StringBuffer &path)
{
    if (path.length()>1 && isPathSepChar(path.charAt(path.length()-1)))
    {
#ifdef _WIN32
    // In addition to not removing / if it's the only char in the path, you should not remove it the path
    // is of the form "c:\"
        if (path.length()>3 || path.charAt(1) != ':')
#endif
            path.remove(path.length()-1, 1);
    }
    return path;
}

inline StringBuffer &addNonEmptyPathSepChar(StringBuffer &path,char sepchar=0)
{
    size32_t len = path.length();
    if (len && !isPathSepChar(path.charAt(len-1)))
        path.append(sepchar?sepchar:getPathSepChar(path.str()));
    return path;
}

inline StringBuffer & addDirectoryPrefix(StringBuffer & target, const char * source, char sepchar=0)
{
    if (source && *source)
        addPathSepChar(target.append(source), sepchar);
    return target;
}

extern jlib_decl const char *pathTail(const char *path);
extern jlib_decl const char *pathExtension(const char * path);

inline const char *splitDirTail(const char *path,StringBuffer &dir)
{
    const char *tail=pathTail(path);
    if (tail)
        dir.append((size32_t)(tail-path),path);
    return tail;        
}

inline bool isAbsolutePath(const char *path)
{   
    if (!path||!*path)
        return false;
    return isPathSepChar(path[0])||((path[1]==':')&&(isPathSepChar(path[2])));
}

extern jlib_decl StringBuffer &makeAbsolutePath(const char *relpath,StringBuffer &out,bool mustExist=false);
extern jlib_decl StringBuffer &makeAbsolutePath(StringBuffer &relpath,bool mustExist=false);
extern jlib_decl StringBuffer &makeAbsolutePath(const char *relpath, const char *basedir, StringBuffer &out);
extern jlib_decl StringBuffer &makePathUniversal(const char *path, StringBuffer &out);
extern jlib_decl const char *splitRelativePath(const char *full,const char *basedir,StringBuffer &reldir); // removes basedir if matches, returns tail and relative dir
extern jlib_decl const char *splitDirMultiTail(const char *multipath,StringBuffer &dir,StringBuffer &tail);
extern jlib_decl StringBuffer &mergeDirMultiTail(const char *dir,const char *tail, StringBuffer &multipath);
extern jlib_decl StringBuffer &removeRelativeMultiPath(const char *full,const char *dir,StringBuffer &reltail); // removes dir if matches, returns relative multipath
extern jlib_decl  bool isSpecialPath(const char *path);
inline const char * skipSpecialPath(const char *path)
{
    if (path&&(*path=='/')&&(path[1]=='>'))
        return path+2;
    return path;
}

extern jlib_decl  int stdIoHandle(const char *path);


extern jlib_decl bool checkFileExists(const char * filename);   // note only local files
extern jlib_decl bool checkDirExists(const char * filename);    // note only local files

extern jlib_decl bool isShareChar(char c);
extern jlib_decl char getShareChar();
extern jlib_decl void setShareChar(char c);

extern jlib_decl StringBuffer &setPathDrive(StringBuffer &filename,unsigned drvnum);    // 0=c, 1=d etc filename can be url or local
extern jlib_decl unsigned getPathDrive(const char *filename);   // 0=c, 1=d etc filename can be url or local
extern jlib_decl StringBuffer &swapPathDrive(StringBuffer &filename,unsigned fromdrvnum,unsigned todrvnum,const char *frommask=NULL,const char *tomask=NULL);

class jlib_decl ExtractedBlobInfo : public CInterface
{
public:
    ExtractedBlobInfo(const char * _filename, offset_t _length, offset_t _offset);
    ExtractedBlobInfo() {}
    void serialize(MemoryBuffer & buffer);
    void deserialize(MemoryBuffer & buffer);

public:
    StringAttr filename;
    offset_t length;
    offset_t offset;
};
typedef CIArrayOf<ExtractedBlobInfo> ExtractedBlobArray;

extern jlib_decl void extractBlobElements(const char * prefix, const RemoteFilename &filename, ExtractedBlobArray & extracted);

extern jlib_decl bool mountDrive(const char *drv,const RemoteFilename &rfn); // linux only currently
extern jlib_decl bool unmountDrive(const char *drv); // linux only currently

extern jlib_decl IFileIO *createUniqueFile(const char *dir, const char *prefix, const char *ext, StringBuffer &tmpName);


// used by remote copy
interface ICopyFileIntercept
{
    virtual offset_t copy(IFileIO *from, IFileIO *to, offset_t ofs, size32_t sz)=0;
};
extern jlib_decl void doCopyFile(IFile * target, IFile * source, size32_t buffersize, ICopyFileProgress *progress, ICopyFileIntercept *copyintercept, bool usetmp);
extern jlib_decl void makeTempCopyName(StringBuffer &tmpname,const char *destname);
extern jlib_decl size32_t SendFile(ISocket *target, IFileIO *fileio,offset_t start,size32_t len);
extern jlib_decl void asyncClose(IFileIO *io);
extern jlib_decl bool containsFileWildcard(const char * path);
extern jlib_decl bool isDirectory(const char * path);

extern jlib_decl IFileIOCache* createFileIOCache(unsigned max);
extern jlib_decl IFile * createSentinelTarget();
extern jlib_decl void writeSentinelFile(IFile * file);
extern jlib_decl void removeSentinelFile(IFile * file);
extern jlib_decl StringBuffer & appendCurrentDirectory(StringBuffer & target, bool blankIfFails);

#endif
