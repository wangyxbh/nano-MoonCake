#include "nano_mooncake/master_protocol.h"

#include <sstream>
#include <stdexcept>
#include <unordered_map>

namespace nano_mooncake {

namespace {

std::string Escape(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  for (char ch : input) {
    if (ch == '\\' || ch == '"' || ch == '|') {
      out.push_back('\\');
    }
    out.push_back(ch);
  }
  return out;
}

std::string Unescape(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  bool escaped = false;
  for (char ch : input) {
    if (escaped) {
      out.push_back(ch);
      escaped = false;
      continue;
    }
    if (ch == '\\') {
      escaped = true;
      continue;
    }
    out.push_back(ch);
  }
  return out;
}

std::vector<std::string> SplitEscaped(const std::string& input, char delim) {
  std::vector<std::string> parts;
  std::string current;
  bool escaped = false;
  for (char ch : input) {
    if (escaped) {
      current.push_back(ch);
      escaped = false;
      continue;
    }
    if (ch == '\\') {
      escaped = true;
      continue;
    }
    if (ch == delim) {
      parts.push_back(current);
      current.clear();
      continue;
    }
    current.push_back(ch);
  }
  parts.push_back(current);
  return parts;
}

std::string SerializeReplica(const ReplicaLocation& replica) {
  std::ostringstream oss;
  oss << Escape(replica.segment_name) << "|"
      << Escape(replica.transport_endpoint) << "|" << replica.offset << "|"
      << replica.length << "|" << Escape(replica.owner_client_id);
  return oss.str();
}

ReplicaLocation ParseReplica(const std::string& input) {
  auto parts = SplitEscaped(input, '|');
  if (parts.size() != 5) {
    throw std::invalid_argument("invalid replica encoding");
  }
  return ReplicaLocation{
      .segment_name = Unescape(parts[0]),
      .transport_endpoint = Unescape(parts[1]),
      .offset = static_cast<std::uint64_t>(std::stoull(parts[2])),
      .length = static_cast<std::size_t>(std::stoull(parts[3])),
      .owner_client_id = Unescape(parts[4]),
  };
}

std::string SerializeReplicas(const std::vector<ReplicaLocation>& replicas) {
  std::ostringstream oss;
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    if (i > 0) {
      oss << ",";
    }
    oss << SerializeReplica(replicas[i]);
  }
  return oss.str();
}

std::vector<ReplicaLocation> ParseReplicas(const std::string& input) {
  if (input.empty()) {
    return {};
  }
  auto entries = SplitEscaped(input, ',');
  std::vector<ReplicaLocation> replicas;
  replicas.reserve(entries.size());
  for (const auto& entry : entries) {
    replicas.push_back(ParseReplica(entry));
  }
  return replicas;
}

std::string SerializeSegmentRecord(const MasterSegmentRecord& record) {
  std::ostringstream oss;
  oss << Escape(record.segment_name) << "|" << Escape(record.transport_endpoint)
      << "|" << record.base_offset << "|" << record.bytes << "|"
      << static_cast<int>(record.status) << "|"
      << Escape(record.owner_client_id);
  return oss.str();
}

MasterSegmentRecord ParseSegmentRecord(const std::string& input) {
  auto parts = SplitEscaped(input, '|');
  if (parts.size() != 6) {
    throw std::invalid_argument("invalid segment record encoding");
  }
  return MasterSegmentRecord{
      .segment_name = Unescape(parts[0]),
      .transport_endpoint = Unescape(parts[1]),
      .base_offset = static_cast<std::uint64_t>(std::stoull(parts[2])),
      .bytes = static_cast<std::size_t>(std::stoull(parts[3])),
      .status = static_cast<SegmentStatus>(std::stoi(parts[4])),
      .owner_client_id = Unescape(parts[5]),
  };
}

std::string SerializeObjectRecord(const ObjectLocationRecord& record) {
  std::ostringstream oss;
  oss << Escape(record.key) << "|" << Escape(record.segment_name) << "|"
      << Escape(record.transport_endpoint) << "|" << record.offset << "|"
      << record.length << "|" << Escape(record.owner_client_id) << "|"
      << SerializeReplicas(record.replicas);
  return oss.str();
}

ObjectLocationRecord ParseObjectRecord(const std::string& input) {
  auto parts = SplitEscaped(input, '|');
  if (parts.size() < 7) {
    throw std::invalid_argument("invalid object record encoding");
  }
  std::string replicas_payload;
  for (std::size_t i = 6; i < parts.size(); ++i) {
    if (i > 6) {
      replicas_payload += "|";
    }
    replicas_payload += parts[i];
  }
  return ObjectLocationRecord{
      .key = Unescape(parts[0]),
      .segment_name = Unescape(parts[1]),
      .transport_endpoint = Unescape(parts[2]),
      .offset = static_cast<std::uint64_t>(std::stoull(parts[3])),
      .length = static_cast<std::size_t>(std::stoull(parts[4])),
      .owner_client_id = Unescape(parts[5]),
      .replicas = ParseReplicas(replicas_payload),
  };
}

std::string JsonEscape(const std::string& input) {
  std::string out;
  out.reserve(input.size() + 8);
  for (char ch : input) {
    switch (ch) {
      case '\\':
      case '"':
        out.push_back('\\');
        out.push_back(ch);
        break;
      case '\n':
        out += "\\n";
        break;
      default:
        out.push_back(ch);
        break;
    }
  }
  return out;
}

std::string ExtractJsonString(const std::string& payload,
                              const std::string& key) {
  const std::string needle = "\"" + key + "\":\"";
  auto pos = payload.find(needle);
  if (pos == std::string::npos) {
    return "";
  }
  pos += needle.size();
  std::string value;
  bool escaped = false;
  for (; pos < payload.size(); ++pos) {
    char ch = payload[pos];
    if (escaped) {
      value.push_back(ch == 'n' ? '\n' : ch);
      escaped = false;
      continue;
    }
    if (ch == '\\') {
      escaped = true;
      continue;
    }
    if (ch == '"') {
      break;
    }
    value.push_back(ch);
  }
  return value;
}

bool ExtractJsonBool(const std::string& payload, const std::string& key) {
  const std::string needle = "\"" + key + "\":";
  auto pos = payload.find(needle);
  if (pos == std::string::npos) {
    return false;
  }
  pos += needle.size();
  return payload.compare(pos, 4, "true") == 0;
}

std::vector<std::string> ExtractJsonArrayStrings(const std::string& payload,
                                                 const std::string& key) {
  const std::string needle = "\"" + key + "\":[";
  auto pos = payload.find(needle);
  if (pos == std::string::npos) {
    return {};
  }
  pos += needle.size();
  auto end = payload.find(']', pos);
  if (end == std::string::npos || end == pos) {
    return {};
  }
  std::string body = payload.substr(pos, end - pos);
  std::vector<std::string> values;
  auto items = SplitEscaped(body, ',');
  for (auto& item : items) {
    if (item.size() >= 2 && item.front() == '"' && item.back() == '"') {
      values.push_back(item.substr(1, item.size() - 2));
    }
  }
  return values;
}

std::unordered_map<std::string, std::string> ParseJsonObjectFields(
    const std::string& payload) {
  std::unordered_map<std::string, std::string> fields;
  auto items = SplitEscaped(payload, ',');
  for (const auto& item : items) {
    auto pos = item.find(':');
    if (pos == std::string::npos) {
      continue;
    }
    auto key = item.substr(0, pos);
    auto value = item.substr(pos + 1);
    if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
      key = key.substr(1, key.size() - 2);
    }
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
      value = value.substr(1, value.size() - 2);
    }
    fields[key] = value;
  }
  return fields;
}

}  // namespace

std::string SerializeMasterRequest(const MasterRequest& request) {
  std::ostringstream oss;
  oss << "{"
      << "\"opcode\":\"" << static_cast<int>(request.opcode) << "\","
      << "\"client_id\":\"" << JsonEscape(request.client_id) << "\","
      << "\"segment_name\":\"" << JsonEscape(request.segment_name) << "\","
      << "\"object_key\":\"" << JsonEscape(request.object_key) << "\","
      << "\"segment\":\"" << JsonEscape(SerializeSegmentRecord(request.segment))
      << "\","
      << "\"object\":\"" << JsonEscape(SerializeObjectRecord(request.object))
      << "\""
      << "}";
  return oss.str();
}

MasterRequest ParseMasterRequest(const std::string& payload) {
  MasterRequest request;
  request.opcode = static_cast<MasterOpcode>(
      std::stoi(ExtractJsonString(payload, "opcode")));
  request.client_id = ExtractJsonString(payload, "client_id");
  request.segment_name = ExtractJsonString(payload, "segment_name");
  request.object_key = ExtractJsonString(payload, "object_key");
  const auto segment_field = ExtractJsonString(payload, "segment");
  if (!segment_field.empty()) {
    request.segment = ParseSegmentRecord(segment_field);
  }
  const auto object_field = ExtractJsonString(payload, "object");
  if (!object_field.empty()) {
    request.object = ParseObjectRecord(object_field);
  }
  return request;
}

std::string SerializeMasterResponse(const MasterResponse& response) {
  std::ostringstream oss;
  oss << "{"
      << "\"ok\":" << (response.ok ? "true" : "false") << ","
      << "\"message\":\"" << JsonEscape(response.message) << "\","
      << "\"segment\":\"";
  if (response.segment.has_value()) {
    oss << JsonEscape(SerializeSegmentRecord(*response.segment));
  }
  oss << "\",\"object\":\"";
  if (response.object.has_value()) {
    oss << JsonEscape(SerializeObjectRecord(*response.object));
  }
  oss << "\",\"segments\":[";
  for (std::size_t i = 0; i < response.segments.size(); ++i) {
    if (i > 0) {
      oss << ",";
    }
    oss << "\"" << JsonEscape(SerializeSegmentRecord(response.segments[i]))
        << "\"";
  }
  oss << "],\"objects\":[";
  for (std::size_t i = 0; i < response.objects.size(); ++i) {
    if (i > 0) {
      oss << ",";
    }
    oss << "\"" << JsonEscape(SerializeObjectRecord(response.objects[i]))
        << "\"";
  }
  oss << "]}";
  return oss.str();
}

MasterResponse ParseMasterResponse(const std::string& payload) {
  MasterResponse response;
  response.ok = ExtractJsonBool(payload, "ok");
  response.message = ExtractJsonString(payload, "message");
  const auto segment_field = ExtractJsonString(payload, "segment");
  if (!segment_field.empty()) {
    response.segment = ParseSegmentRecord(segment_field);
  }
  const auto object_field = ExtractJsonString(payload, "object");
  if (!object_field.empty()) {
    response.object = ParseObjectRecord(object_field);
  }
  for (const auto& item : ExtractJsonArrayStrings(payload, "segments")) {
    response.segments.push_back(ParseSegmentRecord(item));
  }
  for (const auto& item : ExtractJsonArrayStrings(payload, "objects")) {
    response.objects.push_back(ParseObjectRecord(item));
  }
  return response;
}

}  // namespace nano_mooncake
