#pragma once
#include <memory>
#include "ParquetTypes.hpp"

namespace clearParquet {

class WriterProperties {
public:
    class Builder {
    public:
        Builder() : _compression(Compression::UNCOMPRESSED) {}
        virtual ~Builder() {}

        std::shared_ptr<WriterProperties> build() {
            return std::shared_ptr<WriterProperties>(new WriterProperties());
        }

        void compression(Compression::type compression) {
            if (compression != Compression::UNCOMPRESSED) {
                throw std::invalid_argument("Support for anything but Compression::UNCOMPRESSED is unsupported.");
            }
            _compression = compression;
        }

    private:
        Compression::type _compression;
    };
};

}  // end namespace clearParquet
