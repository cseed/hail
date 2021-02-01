#ifndef HAIL_QUERY_BACKEND_STYPE_HPP_INCLUDED
#define HAIL_QUERY_BACKEND_STYPE_HPP_INCLUDED 1

#include <llvm/IR/Value.h>
#include <hail/type.hpp>
#include <vector>

namespace hail {

class SValue;
class EmitValue;

enum class PrimitiveType {
  VOID,
  INT8,
  INT32,
  INT64,
  FLOAT32,
  FLOAT64,
  POINTER
};

class SType {
public:
  using BaseType = SType;
  enum class Tag {
    VOID,
    BOOL,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    STR,
    ARRAY,
    STREAM,
    TUPLE
  };
  const Tag tag;
  const Type *const type;
  SType(Tag tag, const Type *type) : tag(tag), type(type) {}
  virtual ~SType();

  // FIXME make return value an iterator?
  // A generator!
  virtual std::vector<PrimitiveType> constituent_types() const = 0;

  SValue *from_llvm_values(const std::vector<llvm::Value *> &llvm_values, size_t i) const;
};

class SBool : public SType {
public:
  static const Tag self_tag = SType::Tag::BOOL;
  SBool(const Type *type);

  std::vector<PrimitiveType> constituent_types() const;
};

class SInt32 : public SType {
public:
  static const Tag self_tag = SType::Tag::INT32;
  SInt32(const Type *type);

  std::vector<PrimitiveType> constituent_types() const;
};

class SInt64 : public SType {
public:
  static const Tag self_tag = SType::Tag::INT64;
  SInt64(const Type *type);

  std::vector<PrimitiveType> constituent_types() const;
};

class SFloat32 : public SType {
public:
  static const Tag self_tag = SType::Tag::FLOAT32;
  SFloat32(const Type *type);

  std::vector<PrimitiveType> constituent_types() const;
};

class SFloat64 : public SType {
public:
  static const Tag self_tag = SType::Tag::FLOAT64;
  SFloat64(const Type *type);

  std::vector<PrimitiveType> constituent_types() const;
};

class EmitType {
public:
  const SType *const stype;

  EmitType(const SType *stype) : stype(stype) {}

  std::vector<PrimitiveType> constituent_types() const;

  static EmitValue from_llvm_values(const std::vector<llvm::Value *> &llvm_values);
};

}

#endif
