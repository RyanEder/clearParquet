#pragma once

#include <iostream>
#include <memory>

namespace clearParquet {

// Shim layer for file output.
class FileOutputStreamImpl {
public:
    bool Open(const std::string& path, bool append = false) {
        if (path.length() == 0) {
            return false;
        }
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
        if (stream->_impl->Open(path, append)) {
            return stream;
        }
        return nullptr;
    }
    const std::string& filename() {
        static std::string empty;
        if (_impl.get() != nullptr) {
            return _impl->path();
        }
        return empty;
    }

private:
    std::unique_ptr<FileOutputStreamImpl> _impl;
};

}  // end namespace clearParquet
