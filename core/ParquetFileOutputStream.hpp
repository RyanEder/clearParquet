#pragma once

#include <iostream>
#include <memory>

namespace clearParquet {

// Shim layer for file output.
class FileOutputStreamImpl {
public:
    bool Open(const std::string& path, bool append = false) {
        _path = path;
        return true;
    }
    const std::string& path() {
        return _path;
    }

private:
    std::string _path;
};

class FileOutputStream {
public:
    FileOutputStream() {
        _impl.reset(new FileOutputStreamImpl());
    };
    static std::shared_ptr<FileOutputStream> Open(const std::string& path, bool append = false) {
        auto stream = std::shared_ptr<FileOutputStream>(new FileOutputStream());
        stream->_impl->Open(path, append);
        return stream;
    }
    const std::string& filename() {
        return _impl->path();
    }

private:
    std::unique_ptr<FileOutputStreamImpl> _impl;
};

}  // end namespace clearParquet
