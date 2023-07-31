#pragma once

#include <iostream>
#include <memory>

namespace clearParquet {
class FileInputStreamImpl {
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

class FileInputStream {
public:
    FileInputStream() {
        _impl.reset(new FileInputStreamImpl());
    }
    static std::shared_ptr<FileInputStream> Open(const std::string& path) {
        auto stream = std::shared_ptr<FileInputStream>(new FileInputStream());
        if (stream->_impl->Open(path)) {
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
    std::unique_ptr<FileInputStreamImpl> _impl;
};

}  // end namespace clearParquet
