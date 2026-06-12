#include "nano_mooncake/mooncake_master.h"

#include <algorithm>
#include <stdexcept>

namespace nano_mooncake {

void mooncake_master::MountSegment(const MasterSegmentRecord& record) {
  if (record.segment_name.empty()) {
    throw std::invalid_argument("segment_name must not be empty");
  }
  if (record.transport_endpoint.empty()) {
    throw std::invalid_argument("transport_endpoint must not be empty");
  }
  std::lock_guard<std::mutex> lock(mu_);
  auto it = segments_.find(record.segment_name);
  if (it != segments_.end()) {
    const auto& existing = it->second;
    if (existing.transport_endpoint == record.transport_endpoint &&
        existing.base_offset == record.base_offset &&
        existing.bytes == record.bytes &&
        existing.owner_client_id == record.owner_client_id) {
      it->second.status = SegmentStatus::kOk;
      return;
    }
    throw std::invalid_argument(
        "segment_name already mounted with different metadata");
  }
  segments_.emplace(record.segment_name, record);
}

void mooncake_master::UnmountSegment(const std::string& segment_name) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = segments_.find(segment_name);
  if (it == segments_.end()) {
    return;
  }
  it->second.status = SegmentStatus::kUnmounted;
  RemoveSegmentObjectsLocked(segment_name);
}

std::optional<MasterSegmentRecord> mooncake_master::ResolveSegment(
    const std::string& segment_name) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = segments_.find(segment_name);
  if (it == segments_.end() || it->second.status != SegmentStatus::kOk) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<MasterSegmentRecord> mooncake_master::ListSegments() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<MasterSegmentRecord> result;
  result.reserve(segments_.size());
  for (const auto& [_, record] : segments_) {
    result.push_back(record);
  }
  return result;
}

void mooncake_master::PutObject(const ObjectLocationRecord& record) {
  if (record.key.empty()) {
    throw std::invalid_argument("key must not be empty");
  }
  if (record.segment_name.empty()) {
    throw std::invalid_argument("segment_name must not be empty");
  }
  std::lock_guard<std::mutex> lock(mu_);
  if (objects_.find(record.key) != objects_.end()) {
    throw std::invalid_argument("object key already exists");
  }
  auto segment_it = segments_.find(record.segment_name);
  if (segment_it == segments_.end() ||
      segment_it->second.status != SegmentStatus::kOk) {
    throw std::invalid_argument("segment_name is not mounted");
  }
  if (record.offset > segment_it->second.bytes ||
      record.length > segment_it->second.bytes - record.offset) {
    throw std::invalid_argument("object range exceeds segment bounds");
  }
  objects_.emplace(record.key, record);
}

std::optional<ObjectLocationRecord> mooncake_master::GetObject(
    const std::string& key) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = objects_.find(key);
  if (it == objects_.end()) {
    return std::nullopt;
  }
  auto segment_it = segments_.find(it->second.segment_name);
  if (segment_it == segments_.end() ||
      segment_it->second.status != SegmentStatus::kOk) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<ObjectLocationRecord> mooncake_master::ListObjects() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<ObjectLocationRecord> result;
  result.reserve(objects_.size());
  for (const auto& [_, record] : objects_) {
    result.push_back(record);
  }
  return result;
}

void mooncake_master::RemoveSegmentObjectsLocked(
    const std::string& segment_name) {
  for (auto it = objects_.begin(); it != objects_.end();) {
    auto& record = it->second;
    record.replicas.erase(
        std::remove_if(
            record.replicas.begin(), record.replicas.end(),
            [&](const ReplicaLocation& replica) {
              return replica.segment_name == segment_name;
            }),
        record.replicas.end());

    if (record.segment_name == segment_name) {
      if (record.replicas.empty()) {
        it = objects_.erase(it);
        continue;
      }
      PromotePrimaryReplicaLocked(record);
    } else if (record.replicas.empty()) {
      // Keep object metadata internally consistent even if callers inserted a
      // non-replicated record manually.
      it = objects_.erase(it);
      continue;
    }

    ++it;
  }
}

void mooncake_master::PromotePrimaryReplicaLocked(ObjectLocationRecord& record) {
  if (record.replicas.empty()) {
    throw std::logic_error("cannot promote primary replica from empty list");
  }

  const auto& replica = record.replicas.front();
  record.segment_name = replica.segment_name;
  record.transport_endpoint = replica.transport_endpoint;
  record.offset = replica.offset;
  record.length = replica.length;
  record.owner_client_id = replica.owner_client_id;
}

}  // namespace nano_mooncake
