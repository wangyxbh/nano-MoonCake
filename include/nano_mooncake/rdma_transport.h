#pragma once

#include <memory>
#include <string>

#include "transport.h"

namespace nano_mooncake {

class RdmaTransportBackend : public Transport {
 public:
  struct Impl;

  RdmaTransportBackend();
  ~RdmaTransportBackend() override;

  std::string name() const override;
  TransportCapabilities capabilities() const override;
  void release_segment(SegmentId segment_id) override;

 private:
  void do_start(const std::string& local_endpoint) override;
  void do_stop() override;
  void do_add_local_buffer(const RegisteredBuffer& buffer) override;
  void do_remove_local_buffer(const RegisteredBuffer& buffer) override;
  void do_remove_local_segment(const MountedSegment& segment) override;
  void do_prepare_segment(RemoteSegmentHandle& segment) override;
  void do_submit_request(const ResolvedTransferRequest& request) override;

  std::unique_ptr<Impl> impl_;
};

}  // namespace nano_mooncake
