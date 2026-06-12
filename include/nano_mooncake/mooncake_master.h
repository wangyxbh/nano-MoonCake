#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace nano_mooncake {

class mooncake_master {
 public:
  void MountSegment(const MasterSegmentRecord& record);
  void UnmountSegment(const std::string& segment_name);
  std::optional<MasterSegmentRecord> ResolveSegment(
      const std::string& segment_name) const;
  std::vector<MasterSegmentRecord> ListSegments() const;

  void PutObject(const ObjectLocationRecord& record);
  std::optional<ObjectLocationRecord> GetObject(const std::string& key) const;
  std::vector<ObjectLocationRecord> ListObjects() const;

 private:
  void RemoveSegmentObjectsLocked(const std::string& segment_name);
  void PromotePrimaryReplicaLocked(ObjectLocationRecord& record);

  mutable std::mutex mu_;
  std::unordered_map<std::string, MasterSegmentRecord> segments_;
  std::unordered_map<std::string, ObjectLocationRecord> objects_;
};

}  // namespace nano_mooncake
