#pragma once
#include <cstdint>

namespace clearParquet {
class ColumnWriter {
   public:
    ColumnWriter() {}
    int64_t Serialize() {
        uint8_t* out_buffer = nullptr;
        uint32_t out_length = 0;

        SerializeToBuffer(this, &out_length, &out_buffer);
        return static_cast<uint64_t>(out_length);
    }

   private:
    void SerializeToBuffer(ColumnWriter* obj, uint32_t* len, uint8_t** buffer) {
        SerializeObject(obj);
    }

    void SerializeObject(ColumnWriter* obj) {}
};
}  // end namespace clearParquet
