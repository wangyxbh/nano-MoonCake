#pragma once

#include <string>
#include "engine.h"
#include <vector>
#include <cstddef>

namespace nano_mooncake{

    class TransportBackend {
        public:
        virtual ~TransportBackend() =default;
        virtual std::string name() const = 0;

        virtual void init(const std::string& local_endpoint) =0;
        virtual void register_memory(const RegisteredBuffer& buffer) = 0;
        virtual void unregister_memory(const RegisteredBuffer& buffer) = 0;


        virtual RemoteSegmentHandle open_segment(const std::string& segment_name) = 0;
        virtual void close_segment(const std::string& segment_name) = 0;
        virtual void submit(BatchId batch_id , const std::vector<TransferRequest>& requests) = 0;

        virtual TransferStatus poll(BatchId batch_id, std::size_t task_id = 0) = 0;

        virtual void close() = 0;
    };


}
