#pragma once
#include <memory>
#include "ParquetTypes.hpp"

namespace clearParquet {

class WriterProperties {
   public:
    class Builder {
       public:
        Builder() {}
        virtual ~Builder() {}

        std::shared_ptr<WriterProperties> build() {
            return std::shared_ptr<WriterProperties>(new WriterProperties());
        }

        void compression(Compression::type compression) {
            _compression = compression;
        }

       private:
        Compression::type _compression;
    };
};

}  // end namespace clearParquet
