#pragma once
#include "ParquetColumnChunk.hpp"
#include "ParquetTypes.hpp"

#include <iostream>
#include <memory>
#include <unordered_map>

namespace clearParquet {

struct ListEncoding {
    enum type { ONE_LEVEL, TWO_LEVEL, THREE_LEVEL };
};

constexpr size_t BITWIDTH_8 = 8;
constexpr size_t BITWIDTH_16 = 16;
constexpr size_t BITWIDTH_32 = 32;
constexpr size_t BITWIDTH_64 = 64;

class Node {
public:
    enum NodeType { PRIMITIVE, GROUP };

    Node(Node::NodeType type, const std::string& name, Repetition::type repetition, ConvertedType::type convertedType, int id = -1)
        : _type(type), _name(name), _repetition(repetition), _convertedType(convertedType), _id(id), _parent(nullptr) {
        switch (_convertedType) {
            case ConvertedType::UTF8:
                _logicalType.__set_STRING(_logicalType.STRING);
                break;
            case ConvertedType::MAP:
            case ConvertedType::MAP_KEY_VALUE:
                _logicalType.__set_MAP(_logicalType.MAP);
                break;
            case ConvertedType::LIST:
                _logicalType.__set_LIST(_logicalType.LIST);
                break;
            case ConvertedType::ENUM:
                _logicalType.__set_ENUM(_logicalType.ENUM);
                break;
            case ConvertedType::DECIMAL:
                // Not implemented
                _logicalType.__set_DECIMAL(_logicalType.DECIMAL);
                break;
            case ConvertedType::DATE:
                _logicalType.__set_DATE(_logicalType.DATE);
                break;
            case ConvertedType::TIME_MILLIS:
            case ConvertedType::TIME_MICROS:
                // Not Implemented
                _logicalType.__set_TIME(_logicalType.TIME);
                break;
            case ConvertedType::TIMESTAMP_MILLIS:
            case ConvertedType::TIMESTAMP_MICROS:
                // Not Implemented
                _logicalType.__set_TIMESTAMP(_logicalType.TIMESTAMP);
                break;
            case ConvertedType::UINT_8:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_8);
                _logicalType.INTEGER.__set_isSigned(false);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::UINT_16:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_16);
                _logicalType.INTEGER.__set_isSigned(false);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::UINT_32:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_32);
                _logicalType.INTEGER.__set_isSigned(false);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::UINT_64:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_64);
                _logicalType.INTEGER.__set_isSigned(false);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::INT_8:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_8);
                _logicalType.INTEGER.__set_isSigned(true);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::INT_16:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_16);
                _logicalType.INTEGER.__set_isSigned(true);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::INT_32:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_32);
                _logicalType.INTEGER.__set_isSigned(true);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::INT_64:
                _logicalType.INTEGER.__set_bitWidth(BITWIDTH_64);
                _logicalType.INTEGER.__set_isSigned(true);
                _logicalType.__set_INTEGER(_logicalType.INTEGER);
                break;
            case ConvertedType::JSON:
                _logicalType.__set_JSON(_logicalType.JSON);
                break;
            case ConvertedType::BSON:
                _logicalType.__set_BSON(_logicalType.BSON);
                break;
            case ConvertedType::INTERVAL:
                _logicalType.__set_UUID(_logicalType.UUID);
                break;
            case ConvertedType::NONE:
            case ConvertedType::NA:
            case ConvertedType::UNDEFINED:
            default:
                break;
        }
    }

    virtual ~Node() {}

    bool IsPrimitive() const {
        return _type == Node::PRIMITIVE;
    }
    bool IsGroup() const {
        return _type == Node::GROUP;
    }
    bool IsOptional() const {
        return _repetition == Repetition::OPTIONAL;
    }
    bool IsRepeated() const {
        return _repetition == Repetition::REPEATED;
    }
    bool IsRequired() const {
        return _repetition == Repetition::REQUIRED;
    }
    virtual bool Equals(const Node* other) const = 0;
    const std::string& Name() const {
        return _name;
    }
    Node::NodeType GetNodeType() const {
        return _type;
    }
    Repetition::type GetRepetitionType() const {
        return _repetition;
    }
    int Id() const {
        return _id;
    }
    const Node* Parent() const {
        return _parent;
    }
    bool EqualsInternal(const Node* other) const {
        return _type == other->_type && _name == other->_name && _repetition == other->_repetition && _convertedType == other->_convertedType;
    }
    void SetParent(const Node* parent) {
        _parent = parent;
    }

    virtual void ToParquet(void* opaque_element) const = 0;

    // Node::Visitor abstract class for walking schemas with the visitor pattern
    class Visitor {
    public:
        virtual ~Visitor() {}

        virtual void Visit(Node* node) = 0;
    };
    class ConstVisitor {
    public:
        virtual ~ConstVisitor() {}

        virtual void Visit(const Node* node) = 0;
    };

    virtual void Visit(Visitor* visitor) = 0;
    virtual void VisitConst(ConstVisitor* visitor) const = 0;

    Node::NodeType _type;
    std::string _name;
    Repetition::type _repetition;
    ConvertedType::type _convertedType;
    LogicalType _logicalType;
    int _id;
    const Node* _parent;
};

class Node;
class PrimitiveNode;

typedef std::shared_ptr<Node> NodePtr;
typedef std::vector<NodePtr> NodeVector;

struct SchemaElement {
    SchemaElement()
        : _type(static_cast<Type::type>(0)),
          _typeLength(0),
          _repetitionType(static_cast<Repetition::type>(0)),
          _name(),
          _numChildren(0),
          _convertedType(static_cast<ConvertedType::type>(0)),
          _scale(0),
          _precision(0),
          _fieldId(0) {}
    Type::type _type;
    int32_t _typeLength;
    Repetition::type _repetitionType;
    std::string _name;
    int32_t _numChildren;
    ConvertedType::type _convertedType;
    int32_t _scale;
    int32_t _precision;
    int32_t _fieldId;
    LogicalType _logicalType;
    _SchemaElement__isset __isset;

    void __set_type(const Type::type val) {
        _type = val;
        __isset._type = true;
    }

    void __set_type_length(const int32_t val) {
        _typeLength = val;
        __isset._typeLength = true;
    }

    void __set_repetition_type(const Repetition::type val) {
        _repetitionType = val;
        __isset._repetitionType = true;
    }

    void __set_name(const std::string& val) {
        _name = val;
    }

    void __set_num_children(const int32_t val) {
        _numChildren = val;
        __isset._numChildren = true;
    }

    void __set_converted_type(const ConvertedType::type val) {
        _convertedType = val;
        __isset._convertedType = true;
    }

    void __set_scale(const int32_t val) {
        _scale = val;
        __isset._scale = true;
    }

    void __set_precision(const int32_t val) {
        _precision = val;
        __isset._precision = true;
    }

    void __set_field_id(const int32_t val) {
        _fieldId = val;
        __isset._fieldId = true;
    }

    void __set_logical_type(const LogicalType& val) {
        _logicalType = val;
        __isset._logicalType = true;
    }

    bool operator==(const SchemaElement& rhs) const {
        if (__isset._type != rhs.__isset._type)
            return false;
        else if (__isset._type && !(_type == rhs._type))
            return false;
        if (__isset._typeLength != rhs.__isset._typeLength)
            return false;
        else if (__isset._typeLength && !(_typeLength == rhs._typeLength))
            return false;
        if (__isset._repetitionType != rhs.__isset._repetitionType)
            return false;
        else if (__isset._repetitionType && !(_repetitionType == rhs._repetitionType))
            return false;
        if (!(_name == rhs._name))
            return false;
        if (__isset._numChildren != rhs.__isset._numChildren)
            return false;
        else if (__isset._numChildren && !(_numChildren == rhs._numChildren))
            return false;
        if (__isset._convertedType != rhs.__isset._convertedType)
            return false;
        else if (__isset._convertedType && !(_convertedType == rhs._convertedType))
            return false;
        if (__isset._scale != rhs.__isset._scale)
            return false;
        else if (__isset._scale && !(_scale == rhs._scale))
            return false;
        if (__isset._precision != rhs.__isset._precision)
            return false;
        else if (__isset._precision && !(_precision == rhs._precision))
            return false;
        if (__isset._fieldId != rhs.__isset._fieldId)
            return false;
        else if (__isset._fieldId && !(_fieldId == rhs._fieldId))
            return false;
        if (__isset._logicalType != rhs.__isset._logicalType)
            return false;
        else if (__isset._logicalType && !(_logicalType == rhs._logicalType))
            return false;
        return true;
    }
    bool operator!=(const SchemaElement& rhs) const {
        return !(*this == rhs);
    }
};

inline std::ostream& operator<<(std::ostream& os, const SchemaElement& obj) {
    os << " Type: " << obj._type << std::endl;
    os << " TypeLen: " << obj._typeLength << std::endl;
    os << " RepType: " << obj._repetitionType << std::endl;
    os << " Name: " << obj._name << std::endl;
    os << " NumChild: " << obj._numChildren << std::endl;
    return os;
}

class PrimitiveNode : public Node {
public:
    static inline NodePtr Make(const std::string& name, Repetition::type repetition, Type::type type, ConvertedType::type convertedType = ConvertedType::NONE,
                               size_t size = 0) {
        return NodePtr(new PrimitiveNode(name, repetition, type, convertedType, size));
    }
    bool EqualsInternal(const PrimitiveNode* other) const {
        bool is_equal = true;
        if ((_type != other->_type) || (_convertedType != other->_convertedType)) {
            return false;
        }
        if (_type == Type::FIXED_LEN_BYTE_ARRAY) {
            is_equal &= (_size == other->_size);
        }
        return is_equal;
    }

    bool Equals(const Node* other) const override {
        if (!Node::EqualsInternal(other)) {
            return false;
        }
        return EqualsInternal(static_cast<const PrimitiveNode*>(other));
    }
    void Visit(Node::Visitor* visitor) override {
        visitor->Visit(this);
    }
    void VisitConst(Node::ConstVisitor* visitor) const override {
        visitor->Visit(this);
    }
    void ToParquet(void* opaque_element) const override {
        SchemaElement* element = static_cast<SchemaElement*>(opaque_element);
        element->__set_name(_name);
        element->__set_repetition_type((FieldRepetitionType::type)(_repetition));
        if (_convertedType != ConvertedType::NONE) {
            element->__set_converted_type(_convertedType);
        }
        element->__set_type(_type);
        if (_type == Type::FIXED_LEN_BYTE_ARRAY) {
            element->__set_type_length(TypeLength());
        }
        if (_logicalType.__is_set()) {
            element->__set_logical_type(_logicalType);
        }
        // Decimals stuff, not implemented.
    }
    size_t TypeLength() const {
        return _size;
    }
    Type::type GetType() const {
        return _type;
    }
    void SetTypeLength(int32_t length) {
        _size = length;
    }

private:
    PrimitiveNode(const std::string& name, Repetition::type repetition, Type::type type, ConvertedType::type convertedType = ConvertedType::NONE,
                  size_t size = 0, int id = -1)
        : Node(Node::PRIMITIVE, name, repetition, convertedType, id), _type(type), _size(size) {}

    Type::type _type;
    size_t _size;
};

class GroupNode : public Node {
public:
    static inline NodePtr Make(const std::string& name, Repetition::type repetition, const NodeVector& fields,
                               ConvertedType::type convertedType = ConvertedType::NONE) {
        return NodePtr(new GroupNode(name, repetition, fields, convertedType));
    }
    NodePtr Field(int i) const {
        return _fields[i];
    }
    int FieldIndex(const std::string& name) const {
        auto search = _fieldNameToIdx.find(name);
        if (search == _fieldNameToIdx.end()) {
            return -1;
        }
        return search->second;
    }

    int FieldIndex(const Node& node) const {
        auto search = _fieldNameToIdx.equal_range(node.Name());
        for (auto it = search.first; it != search.second; ++it) {
            const int idx = it->second;
            if (&node == Field(idx).get()) {
                return idx;
            }
        }
        return -1;
    }

    int FieldCount() const {
        return static_cast<int>(_fields.size());
    }

    bool EqualsInternal(const GroupNode* other) const {
        if (this == other) {
            return true;
        }
        if (this->FieldCount() != other->FieldCount()) {
            return false;
        }
        for (int i = 0; i < this->FieldCount(); ++i) {
            if (!this->Field(i)->Equals(other->Field(i).get())) {
                return false;
            }
        }
        return true;
    }

    bool Equals(const Node* other) const override {
        if (!Node::EqualsInternal(other)) {
            return false;
        }
        return EqualsInternal(static_cast<const GroupNode*>(other));
    }

    void ToParquet(void* opaque_element) const override {
        SchemaElement* element = static_cast<SchemaElement*>(opaque_element);
        element->__set_name(_name);
        element->__set_num_children(FieldCount());
        element->__set_repetition_type((FieldRepetitionType::type)(_repetition));
        if (_convertedType != ConvertedType::NONE) {
            element->__set_converted_type(_convertedType);
        }
    }
    void Visit(Node::Visitor* visitor) override {
        visitor->Visit(this);
    }
    void VisitConst(Node::ConstVisitor* visitor) const override {
        visitor->Visit(this);
    }

private:
    GroupNode(const std::string& name, Repetition::type repetition, const NodeVector& fields, ConvertedType::type convertedType = ConvertedType::NONE,
              int id = -1)
        : Node(Node::GROUP, name, repetition, convertedType, id), _fields(fields) {
        for (NodePtr& field : _fields) {
            field->SetParent(this);
        }
    }

private:
    NodeVector _fields;
    std::unordered_multimap<std::string, int> _fieldNameToIdx;
};

typedef std::shared_ptr<GroupNode> ParquetSchema;

// End Base Schema classes

class SchemaVisitor : public Node::ConstVisitor {
public:
    explicit SchemaVisitor(std::vector<SchemaElement>* elements) : _elements(elements) {}

    void Visit(const Node* node) override {
        SchemaElement element;
        node->ToParquet(&element);
        _elements->push_back(element);

        if (node->IsGroup()) {
            const GroupNode* group_node = static_cast<const GroupNode*>(node);
            for (int i = 0; i < group_node->FieldCount(); ++i) {
                group_node->Field(i)->VisitConst(this);
            }
        }
    }

private:
    std::vector<SchemaElement>* _elements;
};

class SchemaFlattener {
public:
    SchemaFlattener(const GroupNode* schema, std::vector<SchemaElement>* out) : _root(schema), _elements(out) {}

    void Flatten() {
        SchemaVisitor visitor(_elements);
        _root->VisitConst(&visitor);
    }

private:
    const GroupNode* _root;
    std::vector<SchemaElement>* _elements;
};

static void PrintRepLevel(Repetition::type repetition, std::ostream& stream) {
    switch (repetition) {
        case Repetition::REQUIRED:
            stream << "required";
            break;
        case Repetition::OPTIONAL:
            stream << "optional";
            break;
        case Repetition::REPEATED:
            stream << "repeated";
            break;
        default:
            break;
    }
}

static void PrintType(const PrimitiveNode* node, std::ostream& stream) {
    switch (node->GetType()) {
        case Type::BOOLEAN:
            stream << "boolean";
            break;
        case Type::INT32:
            stream << "int32";
            break;
        case Type::INT64:
            stream << "int64";
            break;
        case Type::INT96:
            stream << "int96";
            break;
        case Type::FLOAT:
            stream << "float";
            break;
        case Type::DOUBLE:
            stream << "double";
            break;
        case Type::BYTE_ARRAY:
            stream << "binary";
            break;
        case Type::FIXED_LEN_BYTE_ARRAY:
            stream << "fixed_len_byte_array(" << node->TypeLength() << ")";
            break;
        default:
            break;
    }
}

class SchemaBuilder : public Node::ConstVisitor {
public:
    explicit SchemaBuilder() {}

    void Visit(const Node* node) override {
        SchemaElement element;
        node->ToParquet((void*)&element);
        _schema.push_back(element);
    }

    void Visit(const GroupNode* node) {
        SchemaElement element;
        node->ToParquet((void*)&element);
        _schema.push_back(element);
        for (int i = 0; i < node->FieldCount(); ++i) {
            node->Field(i)->VisitConst(this);
        }
    }

    std::vector<SchemaElement>& GetSchemas() {
        return _schema;
    }

private:
    std::vector<SchemaElement> _schema;
};

class SchemaPrinter : public Node::ConstVisitor {
public:
    explicit SchemaPrinter(std::ostream& stream, int indent_width) : _stream(stream), _indent(0), _indentWidth(2) {}

    void Visit(const Node* node) override;

private:
    void Visit(const PrimitiveNode* node) {
        PrintRepLevel(node->GetRepetitionType(), _stream);
        _stream << " ";
        PrintType(node, _stream);
        _stream << " " << node->Name();
        _stream << ";" << std::endl;
    }
    void Visit(const GroupNode* node) {}

    void Indent() {
        if (_indent > 0) {
            std::string spaces(_indent, ' ');
            _stream << spaces;
        }
    }

    std::ostream& _stream;

    int _indent;
    int _indentWidth;
};

// Aux for FileMetaData
class EncryptionAlgorithm {
public:
    EncryptionAlgorithm() {}
    virtual ~EncryptionAlgorithm() {}
    bool operator==(const EncryptionAlgorithm& rhs) const {
        return true;
    }
    bool operator!=(const EncryptionAlgorithm& rhs) const {
        return false;
    }
};

class SortingColumn {
public:
    SortingColumn() {}
    virtual ~SortingColumn() {}
    bool operator==(const SortingColumn& rhs) const {
        return true;
    }
    bool operator!=(const SortingColumn& rhs) const {
        return false;
    }
};

class RowGroup {
public:
    RowGroup() : _totalByteSize(0), _numRows(0), _fileOffset(0), _totalCompressedSize(0), _ordinal(0) {}

    virtual ~RowGroup() {}

    std::vector<ColumnChunk> _columns;
    int64_t _totalByteSize;
    int64_t _numRows;
    std::vector<SortingColumn> _sortingColumns;
    int64_t _fileOffset;
    int64_t _totalCompressedSize;
    int16_t _ordinal;

    _RowGroup__isset __isset;

    void __set_columns(const std::vector<ColumnChunk>& val) {
        _columns = val;
    }

    void __set_total_byte_size(const int64_t val) {
        _totalByteSize = val;
    }

    void __set_num_rows(const int64_t val) {
        _numRows = val;
    }

    void __set_sorting_columns(const std::vector<SortingColumn>& val) {
        _sortingColumns = val;
        __isset._sortingColumns = true;
    }

    void __set_file_offset(const int64_t val) {
        _fileOffset = val;
        __isset._fileOffset = true;
    }

    void __set_total_compressed_size(const int64_t val) {
        _totalCompressedSize = val;
        __isset._totalCompressedSize = true;
    }

    void __set_ordinal(const int16_t val) {
        _ordinal = val;
        __isset._ordinal = true;
    }

    bool operator==(const RowGroup& rhs) const {
        if (!(_columns == rhs._columns))
            return false;
        if (!(_totalByteSize == rhs._totalByteSize))
            return false;
        if (!(_numRows == rhs._numRows))
            return false;
        if (__isset._sortingColumns != rhs.__isset._sortingColumns)
            return false;
        else if (__isset._sortingColumns && !(_sortingColumns == rhs._sortingColumns))
            return false;
        if (__isset._fileOffset != rhs.__isset._fileOffset)
            return false;
        else if (__isset._fileOffset && !(_fileOffset == rhs._fileOffset))
            return false;
        if (__isset._totalCompressedSize != rhs.__isset._totalCompressedSize)
            return false;
        else if (__isset._totalCompressedSize && !(_totalCompressedSize == rhs._totalCompressedSize))
            return false;
        if (__isset._ordinal != rhs.__isset._ordinal)
            return false;
        else if (__isset._ordinal && !(_ordinal == rhs._ordinal))
            return false;
        return true;
    }
    bool operator!=(const RowGroup& rhs) const {
        return !(*this == rhs);
    }
};

std::ostream& operator<<(std::ostream& os, const RowGroup& obj) {
    for (const auto& chunk : obj._columns) {
        os << chunk << std::endl;
    }
    os << " TotalByteSize: " << obj._totalByteSize << std::endl;
    os << " NumRows: " << obj._numRows << std::endl;
    os << " FileOffset: " << obj._fileOffset << std::endl;
    os << " TotalCompressedSize: " << obj._totalCompressedSize << std::endl;
    return os;
}

class TypeDefinedOrder {
public:
    TypeDefinedOrder() {}
    virtual ~TypeDefinedOrder() {}
    bool operator==(const TypeDefinedOrder& rhs) const {
        return true;
    }
    bool operator!=(const TypeDefinedOrder& rhs) const {
        return false;
    }
};

class ColumnOrder {
public:
    ColumnOrder() {}
    virtual ~ColumnOrder() {}

    TypeDefinedOrder TYPE_ORDER;

    _ColumnOrder__isset __isset;

    void __set_TYPE_ORDER(const TypeDefinedOrder& val) {
        TYPE_ORDER = val;
        __isset.TYPE_ORDER = true;
    }

    bool operator==(const ColumnOrder& rhs) const {
        if (__isset.TYPE_ORDER != rhs.__isset.TYPE_ORDER)
            return false;
        else if (__isset.TYPE_ORDER && !(TYPE_ORDER == rhs.TYPE_ORDER))
            return false;
        return true;
    }
    bool operator!=(const ColumnOrder& rhs) const {
        return !(*this == rhs);
    }
};

class FileMetaData {
public:
    FileMetaData() : _version(0), _numRows(0), _createdBy(), _footerSigningKeyMetadata() {}

    virtual ~FileMetaData() {}
    int32_t _version;
    std::vector<SchemaElement> _schema;
    int64_t _numRows;
    std::vector<RowGroup> _rowGroups;
    std::vector<KeyValue> _keyValueMetadata;
    std::string _createdBy;
    std::vector<ColumnOrder> _columnOrders;
    EncryptionAlgorithm _encryptionAlgorithm;
    std::string _footerSigningKeyMetadata;

    _FileMetaData__isset __isset;

    void __set_version(const int32_t val) {
        _version = val;
    }

    void __set_schema(const std::vector<SchemaElement>& val) {
        _schema = val;
    }

    void __set_num_rows(const int64_t val) {
        _numRows = val;
    }

    void __set_row_groups(const std::vector<RowGroup>& val) {
        _rowGroups = val;
    }

    void __set_key_value_metadata(const std::vector<KeyValue>& val) {
        _keyValueMetadata = val;
        __isset._keyValueMetadata = true;
    }

    void __set_created_by(const std::string& val) {
        _createdBy = val;
        __isset._createdBy = true;
    }

    void __set_column_orders(const std::vector<ColumnOrder>& val) {
        _columnOrders = val;
        __isset._columnOrders = true;
    }

    void __set_encryption_algorithm(const EncryptionAlgorithm& val) {
        _encryptionAlgorithm = val;
        __isset._encryptionAlgorithm = true;
    }

    void __set_footer_signing_key_metadata(const std::string& val) {
        _footerSigningKeyMetadata = val;
        __isset._footerSigningKeyMetadata = true;
    }

    bool operator==(const FileMetaData& rhs) const {
        if (!(_version == rhs._version))
            return false;
        if (!(_schema == rhs._schema))
            return false;
        if (!(_numRows == rhs._numRows))
            return false;
        if (!(_rowGroups == rhs._rowGroups))
            return false;
        if (__isset._keyValueMetadata != rhs.__isset._keyValueMetadata)
            return false;
        else if (__isset._keyValueMetadata && !(_keyValueMetadata == rhs._keyValueMetadata))
            return false;
        if (__isset._createdBy != rhs.__isset._createdBy)
            return false;
        else if (__isset._createdBy && !(_createdBy == rhs._createdBy))
            return false;
        if (__isset._columnOrders != rhs.__isset._columnOrders)
            return false;
        else if (__isset._columnOrders && !(_columnOrders == rhs._columnOrders))
            return false;
        if (__isset._encryptionAlgorithm != rhs.__isset._encryptionAlgorithm)
            return false;
        else if (__isset._encryptionAlgorithm && !(_encryptionAlgorithm == rhs._encryptionAlgorithm))
            return false;
        if (__isset._footerSigningKeyMetadata != rhs.__isset._footerSigningKeyMetadata)
            return false;
        else if (__isset._footerSigningKeyMetadata && !(_footerSigningKeyMetadata == rhs._footerSigningKeyMetadata))
            return false;
        return true;
    }
    bool operator!=(const FileMetaData& rhs) const {
        return !(*this == rhs);
    }
};

std::ostream& operator<<(std::ostream& os, const FileMetaData& obj) {
    os << "Version: " << obj._version << std::endl;
    os << "Schema: " << std::endl;
    for (const auto& schema : obj._schema) {
        os << schema << std::endl;
    }
    os << "NumRows: " << obj._numRows << std::endl;
    os << "RowGroups: " << std::endl;
    for (const auto& rowGroup : obj._rowGroups) {
        os << rowGroup << std::endl;
    }
    os << "CreatedBy: " << obj._createdBy << std::endl;
    return os;
}

}  // end namespace clearParquet
