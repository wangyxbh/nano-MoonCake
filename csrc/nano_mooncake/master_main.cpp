#include <chrono>
#include <iostream>
#include <thread>

#include "nano_mooncake/master_server.h"

int main(int argc, char** argv) {
  std::string listen_endpoint = "tcp://127.0.0.1:19999";
  if (argc > 1) {
    listen_endpoint = argv[1];
  }

  nano_mooncake::MasterServer server;
  try {
    server.Start(listen_endpoint);
    std::cout << "mooncake_master listening on " << listen_endpoint << std::endl;
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(60));
    }
  } catch (const std::exception& ex) {
    std::cerr << "mooncake_master failed: " << ex.what() << std::endl;
    return 1;
  }
}
