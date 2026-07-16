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

#include "arrowio.hpp"
#include "jfile.hpp"
#include "jlog.hpp"

#include <algorithm>

namespace parquetembed
{

arrow::Status hpccExceptionToStatus(IException *e)
{
    StringBuffer msg;
    e->errorMessage(msg);
    e->Release();
    return arrow::Status::IOError(msg.str());
}

//--------------------------------------------------------------------------
// HpccRandomAccessFile
//--------------------------------------------------------------------------

HpccRandomAccessFile::HpccRandomAccessFile(IFileIO *_fileio)
    : fileio(_fileio)
{
}

HpccRandomAccessFile::~HpccRandomAccessFile()
{
    if (!isClosed)
    {
        try
        {
            fileio->close();
        }
        catch (IException *e)
        {
            EXCLOG(e, "Parquet: error closing file in destructor");
            e->Release();
        }
    }
}

arrow::Status HpccRandomAccessFile::Close()
{
    if (!isClosed)
    {
        try
        {
            fileio->close();
        }
        catch (IException *e)
        {
            return hpccExceptionToStatus(e);
        }
        isClosed = true;
    }
    return arrow::Status::OK();
}

arrow::Result<int64_t> HpccRandomAccessFile::Tell() const
{
    if (isClosed)
        return arrow::Status::IOError("File is closed");
    return position;
}

bool HpccRandomAccessFile::closed() const
{
    return isClosed;
}

arrow::Status HpccRandomAccessFile::Seek(int64_t pos)
{
    if (isClosed)
        return arrow::Status::IOError("File is closed");
    if (pos < 0)
        return arrow::Status::Invalid("Negative seek position");
    position = pos;
    return arrow::Status::OK();
}

arrow::Result<int64_t> HpccRandomAccessFile::Read(int64_t nbytes, void *out)
{
    ARROW_ASSIGN_OR_RAISE(auto bytesRead, ReadAt(position, nbytes, out));
    position += bytesRead;
    return bytesRead;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> HpccRandomAccessFile::Read(int64_t nbytes)
{
    if (nbytes < 0)
        return arrow::Status::Invalid("Negative read size");
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytesRead, false));
    return std::move(buffer);
}

arrow::Result<int64_t> HpccRandomAccessFile::GetSize()
{
    if (isClosed)
        return arrow::Status::IOError("File is closed");
    try
    {
        return static_cast<int64_t>(fileio->size());
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Result<int64_t> HpccRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void *out)
{
    if (isClosed)
        return arrow::Status::IOError("File is closed");
    if (position < 0)
        return arrow::Status::Invalid("Negative read position");
    if (nbytes < 0)
        return arrow::Status::Invalid("Negative read size");
    if (nbytes == 0)
        return 0;
    if (!out)
        return arrow::Status::Invalid("Output buffer is null");
    try
    {
        int64_t totalRead = 0;
        byte *dest = static_cast<byte *>(out);
        while (totalRead < nbytes)
        {
            size32_t toRead = static_cast<size32_t>(std::min(nbytes - totalRead, (int64_t)0x7FFFFFFF));
            size32_t got = fileio->read(position + totalRead, toRead, dest);
            if (got == 0)
                break;
            totalRead += got;
            dest += got;
        }
        return totalRead;
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> HpccRandomAccessFile::ReadAt(int64_t position, int64_t nbytes)
{
    if (position < 0)
        return arrow::Status::Invalid("Negative read position");
    if (nbytes < 0)
        return arrow::Status::Invalid("Negative read size");
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, ReadAt(position, nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytesRead, false));
    return std::move(buffer);
}

//--------------------------------------------------------------------------
// HpccOutputStream
//--------------------------------------------------------------------------

HpccOutputStream::HpccOutputStream(IFileIO *_fileio)
    : fileio(_fileio)
{
}

HpccOutputStream::~HpccOutputStream()
{
    if (!isClosed)
    {
        try
        {
            fileio->flush();
            fileio->close();
        }
        catch (IException *e)
        {
            EXCLOG(e, "Parquet: error closing output stream in destructor");
            e->Release();
        }
    }
}

arrow::Status HpccOutputStream::Close()
{
    if (!isClosed)
    {
        try
        {
            fileio->flush();
            fileio->close();
        }
        catch (IException *e)
        {
            return hpccExceptionToStatus(e);
        }
        isClosed = true;
    }
    return arrow::Status::OK();
}

arrow::Result<int64_t> HpccOutputStream::Tell() const
{
    if (isClosed)
        return arrow::Status::IOError("Stream is closed");
    return position;
}

bool HpccOutputStream::closed() const
{
    return isClosed;
}

arrow::Status HpccOutputStream::Write(const void *data, int64_t nbytes)
{
    if (isClosed)
        return arrow::Status::IOError("Stream is closed");
    if (nbytes < 0)
        return arrow::Status::Invalid("Negative write size");
    if (nbytes == 0)
        return arrow::Status::OK();
    if (!data)
        return arrow::Status::Invalid("Input buffer is null");
    try
    {
        const byte *src = static_cast<const byte *>(data);
        int64_t totalWritten = 0;
        while (totalWritten < nbytes)
        {
            size32_t toWrite = static_cast<size32_t>(std::min(nbytes - totalWritten, (int64_t)0x7FFFFFFF));
            size32_t written = fileio->write(position, toWrite, src);
            position += written;
            totalWritten += written;
            src += written;
            if (written == 0)
                return arrow::Status::IOError("Write returned 0 bytes written");
        }
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccOutputStream::Flush()
{
    if (isClosed)
        return arrow::Status::IOError("Stream is closed");
    try
    {
        fileio->flush();
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

//--------------------------------------------------------------------------
// HpccFileSystem
//--------------------------------------------------------------------------

HpccFileSystem::HpccFileSystem()
    : arrow::fs::FileSystem(arrow::io::default_io_context())
{
}

HpccFileSystem::~HpccFileSystem()
{
}

std::string HpccFileSystem::type_name() const
{
    return "hpcc";
}

bool HpccFileSystem::Equals(const arrow::fs::FileSystem &other) const
{
    return other.type_name() == "hpcc";
}

arrow::Result<arrow::fs::FileInfo> HpccFileSystem::GetFileInfo(const std::string &path)
{
    arrow::fs::FileInfo info;
    info.set_path(path);
    try
    {
        Owned<IFile> file = createIFile(path.c_str());
        if (!file->exists())
        {
            info.set_type(arrow::fs::FileType::NotFound);
            return info;
        }
        if (file->isDirectory() == fileBool::foundYes)
        {
            info.set_type(arrow::fs::FileType::Directory);
        }
        else if (file->isFile() == fileBool::foundYes)
        {
            info.set_type(arrow::fs::FileType::File);
            info.set_size(static_cast<int64_t>(file->size()));
            CDateTime modTime;
            if (file->getTime(nullptr, &modTime, nullptr))
            {
                time_t epochTime = modTime.getSimple();
                auto tp = std::chrono::system_clock::from_time_t(epochTime);
                info.set_mtime(arrow::fs::TimePoint(std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch())));
            }
        }
        else
        {
            info.set_type(arrow::fs::FileType::Unknown);
        }
        return info;
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Result<arrow::fs::FileInfoVector> HpccFileSystem::GetFileInfo(const arrow::fs::FileSelector &select)
{
    arrow::fs::FileInfoVector results;
    try
    {
        Owned<IFile> dir = createIFile(select.base_dir.c_str());
        if (!dir->exists())
        {
            if (select.allow_not_found)
                return results;
            return arrow::Status::IOError("Directory not found: ", select.base_dir);
        }
        if (dir->isDirectory() != fileBool::foundYes)
            return arrow::Status::IOError("Not a directory: ", select.base_dir);

        Owned<IDirectoryIterator> iter = dir->directoryFiles(nullptr, select.recursive, true);
        ForEach(*iter)
        {
            IFile &entry = iter->query();
            arrow::fs::FileInfo info;
            info.set_path(entry.queryFilename());
            if (iter->isDir())
            {
                info.set_type(arrow::fs::FileType::Directory);
            }
            else
            {
                info.set_type(arrow::fs::FileType::File);
                info.set_size(static_cast<int64_t>(iter->getFileSize()));
                CDateTime modTime;
                iter->getModifiedTime(modTime);
                time_t epochTime = modTime.getSimple();
                auto tp = std::chrono::system_clock::from_time_t(epochTime);
                info.set_mtime(arrow::fs::TimePoint(std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch())));
            }
            results.push_back(std::move(info));
        }
        return results;
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccFileSystem::CreateDir(const std::string &path, bool recursive)
{
    try
    {
        if (recursive)
        {
            recursiveCreateDirectory(path.c_str());
        }
        else
        {
            Owned<IFile> dir = createIFile(path.c_str());
            dir->createDirectory();
        }
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccFileSystem::DeleteDir(const std::string &path)
{
    try
    {
        recursiveRemoveDirectory(path.c_str());
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccFileSystem::DeleteDirContents(const std::string &path, bool missing_dir_ok)
{
    if (path.empty() || path == "/")
        return arrow::Status::Invalid("Cannot delete root directory contents");
    try
    {
        Owned<IFile> dir = createIFile(path.c_str());
        if (!dir->exists())
        {
            if (missing_dir_ok)
                return arrow::Status::OK();
            return arrow::Status::IOError("Directory not found: ", path);
        }
        Owned<IDirectoryIterator> iter = dir->directoryFiles(nullptr, false, true);
        ForEach(*iter)
        {
            IFile &entry = iter->query();
            if (iter->isDir())
                recursiveRemoveDirectory(&entry);
            else
                entry.remove();
        }
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccFileSystem::DeleteRootDirContents()
{
    return arrow::Status::Invalid("Deleting root directory contents is not supported");
}

arrow::Status HpccFileSystem::DeleteFile(const std::string &path)
{
    try
    {
        Owned<IFile> file = createIFile(path.c_str());
        if (!file->remove())
            return arrow::Status::IOError("Failed to delete file: ", path);
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccFileSystem::Move(const std::string &src, const std::string &dest)
{
    try
    {
        Owned<IFile> srcFile = createIFile(src.c_str());
        srcFile->move(dest.c_str());
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Status HpccFileSystem::CopyFile(const std::string &src, const std::string &dest)
{
    try
    {
        Owned<IFile> srcFile = createIFile(src.c_str());
        Owned<IFile> destFile = createIFile(dest.c_str());
        srcFile->copyTo(destFile);
        return arrow::Status::OK();
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Result<std::shared_ptr<arrow::io::InputStream>> HpccFileSystem::OpenInputStream(const std::string &path)
{
    return OpenInputFile(path);
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> HpccFileSystem::OpenInputFile(const std::string &path)
{
    try
    {
        Owned<IFile> file = createIFile(path.c_str());
        Owned<IFileIO> fileio = file->open(IFOread);
        if (!fileio)
            return arrow::Status::IOError("Failed to open file for reading: ", path);
        return std::make_shared<HpccRandomAccessFile>(fileio.getClear());
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>> HpccFileSystem::OpenOutputStream(
    const std::string &path,
    const std::shared_ptr<const arrow::KeyValueMetadata> &metadata)
{
    (void) metadata;
    try
    {
        recursiveCreateDirectoryForFile(path.c_str());
        Owned<IFile> file = createIFile(path.c_str());
        Owned<IFileIO> fileio = file->open(IFOcreate);
        if (!fileio)
            return arrow::Status::IOError("Failed to open file for writing: ", path);
        return std::make_shared<HpccOutputStream>(fileio.getClear());
    }
    catch (IException *e)
    {
        return hpccExceptionToStatus(e);
    }
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>> HpccFileSystem::OpenAppendStream(
    const std::string &path,
    const std::shared_ptr<const arrow::KeyValueMetadata> &metadata)
{
    (void) path;
    (void) metadata;
    return arrow::Status::NotImplemented("Append streams are not supported by HpccFileSystem");
}

} // namespace parquetembed
