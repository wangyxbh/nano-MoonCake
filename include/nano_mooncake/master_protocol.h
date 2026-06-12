#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "types.h"

namespace nano_mooncake {

enum class MasterOpcode : uint8_t {
  kMountSegment = 0,
  kUnmountSegment = 1,
  kResolveSegment = 2,
  kPutObject = 3,
  kGetObject = 4,
  kListSegments = 5,
  kListObjects = 6,
};

struct MasterRequest {
  MasterOpcode opcode = MasterOpcode::kResolveSegment;
  std::uint64_t trace_id = 0;
  std::string client_id;
  std::string segment_name;
  std::string object_key;
  MasterSegmentRecord segment;
  ObjectLocationRecord object;
};

struct MasterResponse {
  bool ok = false;
  std::string message;
  std::optional<MasterSegmentRecord> segment;
  std::optional<ObjectLocationRecord> object;
  std::vector<MasterSegmentRecord> segments;
  std::vector<ObjectLocationRecord> objects;
};

std::string SerializeMasterRequest(const MasterRequest& request);
MasterRequest ParseMasterRequest(const std::string& payload);

std::string SerializeMasterResponse(const MasterResponse& response);
MasterResponse ParseMasterResponse(const std::string& payload);

}  // namespace nano_mooncake
