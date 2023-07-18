#pragma once
#include <memory>
#include <stdexcept>
#include "ParquetTypes.hpp"

namespace clearParquet {

class WriterProperties {
public:
    WriterProperties(Compression::type compression) : _compression(compression) {}
    class Builder {
    public:
        Builder() : _compression(Compression::UNCOMPRESSED) {}
        virtual ~Builder() {}

        std::shared_ptr<WriterProperties> build() {
            return std::shared_ptr<WriterProperties>(new WriterProperties(_compression));
        }

        void enable_dictionary() {  // Unsupported.
        }
        void disable_dictionary() {  // Not enabled by default.
        }
        void enable_page_checksum() {  // Unsupported.
        }
        void disable_page_checksum() {  // Not enabled by default.
        }
        void enable_statistics() {  // Unsupported.
        }
        void disable_statistics() {  // Not enabled by default.
        }

        void compression(Compression::type compression) {
            if (compression != Compression::UNCOMPRESSED
#if defined(PARQUET_SNAPPY_COMPRESSION)
                && compression != Compression::SNAPPY
#endif
#if defined(PARQUET_ZSTD_COMPRESSION)
                && compression != Compression::ZSTD
#endif
            ) {
                throw std::invalid_argument(
                    "Support for anything but Compression::UNCOMPRESSED"
#if defined(PARQUET_SNAPPY_COMPRESSION)
                    ", Compression::SNAPPY"
#endif
#if defined(PARQUET_ZSTD_COMPRESSION)
                    ", Compression::ZSTD"
#endif
                    " is unsupported.");
            }
            _compression = compression;
        }

    private:
        Compression::type _compression;
    };

public:
    Compression::type getCompression() {
        return _compression;
    }

private:
    Compression::type _compression;
};

}  // end namespace clearParquet
