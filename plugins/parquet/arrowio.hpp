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

#pragma once

#include "arrow/io/interfaces.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "jfile.hpp"
#include "jmutex.hpp"

namespace parquetembed
{

// Convert a caught HPCC IException into an arrow::Status::IOError. Releases the exception.
arrow::Status hpccExceptionToStatus(IException *e);

/**
 * @brief Adapter that wraps an HPCC IFileIO for random-access reading,
 * presenting it as an arrow::io::RandomAccessFile.
 *
 * This allows the Arrow/Parquet SDK to read from any HPCC file source
 * (local, remote via dafilesrv, etc.) through the standard IFile infrastructure.
 */
class HpccRandomAccessFile : public arrow::io::RandomAccessFile
{
public:
    explicit HpccRandomAccessFile(IFileIO *_fileio);
    ~HpccRandomAccessFile() override;

    // FileInterface
    arrow::Status Close() override;
    arrow::Result<int64_t> Tell() const override;
    bool closed() const override;

    // Seekable
    arrow::Status Seek(int64_t pos) override;

    // Readable
    arrow::Result<int64_t> Read(int64_t nbytes, void *out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

    // RandomAccessFile
    arrow::Result<int64_t> GetSize() override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

private:
    Owned<IFileIO> fileio;
    int64_t position = 0;
    bool isClosed = false;
};

/**
 * @brief Adapter that wraps an HPCC IFileIO for sequential writing,
 * presenting it as an arrow::io::OutputStream.
 *
 * This allows the Arrow/Parquet SDK to write to any HPCC file destination
 * (local, remote via dafilesrv, etc.) through the standard IFile infrastructure.
 */
class HpccOutputStream : public arrow::io::OutputStream
{
public:
    explicit HpccOutputStream(IFileIO *_fileio);
    ~HpccOutputStream() override;

    // FileInterface
    arrow::Status Close() override;
    arrow::Result<int64_t> Tell() const override;
    bool closed() const override;

    // Writable
    arrow::Status Write(const void *data, int64_t nbytes) override;
    arrow::Status Flush() override;

private:
    Owned<IFileIO> fileio;
    int64_t position = 0;
    bool isClosed = false;
};

/**
 * @brief Adapter that wraps the HPCC IFile infrastructure as an arrow::fs::FileSystem.
 *
 * Used by the Arrow Dataset API for partitioned dataset operations (directory traversal,
 * opening files for reading/writing). Delegates all file operations to HPCC's createIFile().
 */
class HpccFileSystem : public arrow::fs::FileSystem
{
public:
    HpccFileSystem();
    ~HpccFileSystem() override;

    std::string type_name() const override;
    bool Equals(const arrow::fs::FileSystem &other) const override;

    /// \cond FALSE
    using FileSystem::DeleteDirContents;
    using FileSystem::GetFileInfo;
    using FileSystem::OpenAppendStream;
    using FileSystem::OpenOutputStream;
    /// \endcond

    // File metadata
    arrow::Result<arrow::fs::FileInfo> GetFileInfo(const std::string &path) override;
    arrow::Result<arrow::fs::FileInfoVector> GetFileInfo(const arrow::fs::FileSelector &select) override;

    // Directory operations
    arrow::Status CreateDir(const std::string &path, bool recursive) override;
    arrow::Status DeleteDir(const std::string &path) override;
    arrow::Status DeleteDirContents(const std::string &path, bool missing_dir_ok) override;
    arrow::Status DeleteRootDirContents() override;

    // File operations
    arrow::Status DeleteFile(const std::string &path) override;
    arrow::Status Move(const std::string &src, const std::string &dest) override;
    arrow::Status CopyFile(const std::string &src, const std::string &dest) override;

    // I/O streams
    arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(const std::string &path) override;
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string &path) override;
    arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
        const std::string &path,
        const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) override;
    arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
        const std::string &path,
        const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) override;
};

} // namespace parquetembed
