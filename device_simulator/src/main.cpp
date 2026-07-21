#include "core/Simulator.h"
#include "core/Config.h"

#include <iostream>
#include <exception>

int main(int argc, char* argv[])
{
    try
    {
        std::string config_path = "config/devices.json";
        if (argc > 1)
        {
            config_path = argv[1];
        }

        std::cout << "=== Device Simulator v1.0 ===" << std::endl;
        std::cout << "Loading config: " << config_path << std::endl;

        auto config = device_sim::Config::LoadFromFile(config_path);

        std::cout << "Collector target: "
                  << config.collector().host << ":" << config.collector().port << std::endl;
        std::cout << "Devices: " << config.devices().size() << " configured" << std::endl;
        std::cout << "Anomalies: " << config.anomalies().size() << " configured" << std::endl;
        std::cout << "================================" << std::endl;

        device_sim::Simulator simulator(std::move(config));
        simulator.Run();

        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "FATAL: " << e.what() << std::endl;
        return 1;
    }
}
