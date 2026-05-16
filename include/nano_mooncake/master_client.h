#pragma once

#include <optional>
#include <string>
#include <vector>

#include "master_protocol.h"

namespace nano_mooncake {

class MasterClient {
 public:
  explicit MasterClient(std::string master_endpoint);

  const std::string& endpoint() const { return master_endpoint_; }

  void MountSegment(const MasterSegmentRecord& segment);
  void UnmountSegment(const std::string& segment_name);
  std::optional<MasterSegmentRecord> ResolveSegment(
      const std::string& segment_name);

  void PutObject(const ObjectLocationRecord& object);
  std::optional<ObjectLocationRecord> GetObject(const std::string& key);

  std::vector<MasterSegmentRecord> ListSegments();
  std::vector<ObjectLocationRecord> ListObjects();

 private:
  MasterResponse RoundTrip(const MasterRequest& request);

  std::string master_endpoint_;
};

}  // namespace nano_mooncake
