#include <cassert>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>

#include "nano_mooncake/engine.h"
#include "nano_mooncake/master_server.h"
#include "nano_mooncake/mooncake_master.h"

namespace {

using nano_mooncake::MasterSegmentRecord;
using nano_mooncake::ObjectLocationRecord;
using nano_mooncake::ReplicaLocation;
using nano_mooncake::SegmentStatus;
using nano_mooncake::mooncake_master;

MasterSegmentRecord MakeSegment(const std::string& name,
                                const std::string& endpoint) {
  return MasterSegmentRecord{
      .segment_name = name,
      .transport_endpoint = endpoint,
      .base_offset = 0,
      .bytes = 4096,
      .status = SegmentStatus::kOk,
      .owner_client_id = "client-" + name,
  };
}

void TestUnmountDropsSingleReplicaObject() {
  mooncake_master master;
  master.MountSegment(MakeSegment("seg-a", "tcp://127.0.0.1:19001"));
  master.PutObject(ObjectLocationRecord{
      .key = "object-a",
      .segment_name = "seg-a",
      .transport_endpoint = "tcp://127.0.0.1:19001",
      .offset = 128,
      .length = 512,
      .owner_client_id = "client-seg-a",
      .replicas =
          {ReplicaLocation{
              .segment_name = "seg-a",
              .transport_endpoint = "tcp://127.0.0.1:19001",
              .offset = 128,
              .length = 512,
              .owner_client_id = "client-seg-a",
          }},
  });

  master.UnmountSegment("seg-a");
  assert(!master.GetObject("object-a").has_value());

  master.MountSegment(MakeSegment("seg-a", "tcp://127.0.0.1:19001"));
  assert(!master.GetObject("object-a").has_value());
}

void TestUnmountPromotesRemainingReplica() {
  mooncake_master master;
  master.MountSegment(MakeSegment("seg-a", "tcp://127.0.0.1:19001"));
  master.MountSegment(MakeSegment("seg-b", "tcp://127.0.0.1:19002"));

  master.PutObject(ObjectLocationRecord{
      .key = "object-b",
      .segment_name = "seg-a",
      .transport_endpoint = "tcp://127.0.0.1:19001",
      .offset = 64,
      .length = 256,
      .owner_client_id = "client-seg-a",
      .replicas =
          {
              ReplicaLocation{
                  .segment_name = "seg-a",
                  .transport_endpoint = "tcp://127.0.0.1:19001",
                  .offset = 64,
                  .length = 256,
                  .owner_client_id = "client-seg-a",
              },
              ReplicaLocation{
                  .segment_name = "seg-b",
                  .transport_endpoint = "tcp://127.0.0.1:19002",
                  .offset = 64,
                  .length = 256,
                  .owner_client_id = "client-seg-b",
              },
          },
  });

  master.UnmountSegment("seg-a");

  auto object = master.GetObject("object-b");
  assert(object.has_value());
  assert(object->segment_name == "seg-b");
  assert(object->transport_endpoint == "tcp://127.0.0.1:19002");
  assert(object->replicas.size() == 1);
  assert(object->replicas.front().segment_name == "seg-b");
}

void TestEndToEndReadObject() {
  nano_mooncake::MasterServer server;
  server.Start("tcp://127.0.0.1:19999");
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  nano_mooncake::Engine provider;
  nano_mooncake::Engine consumer;

  provider.start("tcp://127.0.0.1:19001", "tcp://127.0.0.1:19999", "provider");
  consumer.start("tcp://127.0.0.1:19002", "tcp://127.0.0.1:19999", "consumer");

  char provider_buffer[256] = {};
  char consumer_buffer[256] = {};
  const std::string payload = "nano-mooncake-e2e";
  std::memcpy(provider_buffer, payload.data(), payload.size());

  auto provider_reg = provider.register_buffer(
      nano_mooncake::BufferView{
          .data = provider_buffer,
          .bytes = sizeof(provider_buffer),
          .device = nano_mooncake::DeviceType::kCPU,
      },
      "cpu", true);
  auto consumer_reg = consumer.register_buffer(
      nano_mooncake::BufferView{
          .data = consumer_buffer,
          .bytes = sizeof(consumer_buffer),
          .device = nano_mooncake::DeviceType::kCPU,
      },
      "cpu", true);

  provider.mount_segment("seg-e2e", provider_reg.buffer_id);
  provider.put_object("obj-e2e", "seg-e2e", 0, payload.size());

  auto handle = consumer.read_object("obj-e2e", consumer_reg.buffer_id);
  auto status = consumer.wait(handle.batch_id, 1000);
  assert(status.state == nano_mooncake::TransferState::kCompleted);
  assert(status.transferred_bytes == payload.size());
  assert(std::string(consumer_buffer, payload.size()) == payload);

  consumer.close();
  provider.close();
  server.Stop();
}

void TestRdmaBuildGuard() {
#ifndef NANO_HAS_RDMA
  nano_mooncake::Engine engine;
  bool threw = false;
  try {
    engine.start("rdma://127.0.0.1:18500", "tcp://127.0.0.1:19999", "rdma-client");
  } catch (const std::runtime_error& ex) {
    threw = std::string(ex.what()).find("without RDMA support") !=
            std::string::npos;
  }
  assert(threw);
#endif
}

}  // namespace

int main() {
  TestUnmountDropsSingleReplicaObject();
  TestUnmountPromotesRemainingReplica();
  TestRdmaBuildGuard();
  TestEndToEndReadObject();
  return 0;
}
