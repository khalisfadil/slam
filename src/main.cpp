#include <pipeline.hpp>

int main() {
    std::string lidar_json = "../config/2025047_1054_OS-2-128_122446000745.json";
    std::string config_json = "../config/odom_config.json";
    uint32_t lidar_packet_size = 24896;

    // Read and parse JSON file to get udp_profile_lidar and udp_port_lidar
    std::string udp_profile_lidar;
    std::string udp_dest;
    std::string udp_dest_all = "192.168.75.10";
    uint16_t udp_port_lidar; 
    // uint16_t udp_port_imu;
    uint16_t udp_port_gnss = 6597;
    uint32_t id20_packet_size = 105;

    // Generate UTC timestamp for filename
    auto now = std::chrono::system_clock::now();
    auto utc_time = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&utc_time), "%Y%m%d_%H%M%S");
    std::string timestamp = ss.str();
    std::string log_filename = "../report/log/log_report_" + timestamp + ".txt";
    std::string gt_filename = "../report/gt/gt_report_" + timestamp + ".txt";

    try {
        std::ifstream json_file(lidar_json);
        if (!json_file.is_open()) {
                throw std::runtime_error("[Main] Error: Could not open JSON file: " + lidar_json);
            return EXIT_FAILURE;
        }
        nlohmann::json metadata_;
        json_file >> metadata_;
        json_file.close(); // Explicitly close the file

        if (!metadata_.contains("lidar_data_format") || !metadata_["lidar_data_format"].is_object()) {
                throw std::runtime_error("Missing or invalid 'lidar_data_format' object");
            return EXIT_FAILURE;
        }
        if (!metadata_.contains("config_params") || !metadata_["config_params"].is_object()) {
                throw std::runtime_error("Missing or invalid 'config_params' object");
            return EXIT_FAILURE;
        }
        if (!metadata_.contains("beam_intrinsics") || !metadata_["beam_intrinsics"].is_object()) {
                throw std::runtime_error("Missing or invalid 'beam_intrinsics' object");
            return EXIT_FAILURE;
        }
        if (!metadata_.contains("lidar_intrinsics") || !metadata_["lidar_intrinsics"].is_object() ||
            !metadata_["lidar_intrinsics"].contains("lidar_to_sensor_transform")) {
                throw std::runtime_error("Missing or invalid 'lidar_intrinsics.lidar_to_sensor_transform'");
            return EXIT_FAILURE;
        }

        udp_profile_lidar = metadata_["config_params"]["udp_profile_lidar"].get<std::string>();
        udp_port_lidar = metadata_["config_params"]["udp_port_lidar"].get<uint16_t>();
        // udp_port_imu = metadata_["config_params"]["udp_port_imu"].get<uint16_t>();
        udp_dest = metadata_["config_params"]["udp_dest"].get<std::string>();

    } catch (const std::exception& e) {
        std::cerr << "[Main] Error parsing JSON: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // ####################### CONFIG ########################
    // Configure the UDP socket
    udp_socket::UdpSocketConfig config_lidar;
    config_lidar.host = "192.168.75.10"; // Listen on interfaces
    config_lidar.port = udp_port_lidar;     // UDP port for sonar data
    config_lidar.bufferSize = lidar_packet_size; // Maximum UDP packet size
    config_lidar.receiveTimeout = std::chrono::milliseconds(1000); // 1-second timeout
    config_lidar.enableBroadcast = false; // Enable broadcast if sonar data is broadcasted
    // config_lidar.multicastGroup = boost::asio::ip::address::from_string("255.255.255.255"); // Multicast group
    // config_lidar.ttl = 1; // Set TTL for multicast
    config_lidar.multicastGroup = std::nullopt; // Multicast group
    config_lidar.ttl = std::nullopt; // Set TTL for multicast
    config_lidar.reuseAddress = true; // Allow port reuse

    // Configure the UDP socket
    udp_socket::UdpSocketConfig config_compass;
    config_compass.host = "192.168.75.10"; // Listen on interfaces
    config_compass.port = udp_port_gnss;     // UDP port for sonar data
    config_compass.bufferSize = id20_packet_size; // Maximum UDP packet size
    config_compass.receiveTimeout = std::chrono::milliseconds(1000); // 1-second timeout
    config_compass.enableBroadcast = false; // Enable broadcast if sonar data is broadcasted
    config_compass.multicastGroup = std::nullopt; // Multicast group
    config_compass.ttl = std::nullopt; // Set TTL for multicast
    config_compass.reuseAddress = true; // Allow port reuse

    // ########################################################
    uint16_t chnl_strd = 2;
    SLAMPipeline pipeline("SLAM_LIDAR_ODOM", config_json, lidar_json, lidarDecode::OusterLidarCallback::LidarTransformPreset::GEHLSDORF20250410, chnl_strd);

    struct sigaction sigIntHandler;
    sigIntHandler.sa_handler = SLAMPipeline::signalHandler;
    sigemptyset(&sigIntHandler.sa_mask);
    sigIntHandler.sa_flags = 0;

    sigaction(SIGINT, &sigIntHandler, nullptr);
    sigaction(SIGTERM, &sigIntHandler, nullptr);

#ifdef DEBUG
    std::cout << "[Main] Starting pipeline processes..." << std::endl;
#endif

    try {
        std::vector<std::thread> threads;
        boost::asio::io_context ioContextPoints;

        // Switch-case based on udp_profile_lidar
        enum class LidarProfile { RNG19_RFL8_SIG16_NIR16, LEGACY, UNKNOWN };
        LidarProfile profile;
        if (udp_profile_lidar == "RNG19_RFL8_SIG16_NIR16") {
            profile = LidarProfile::RNG19_RFL8_SIG16_NIR16;
            lidar_packet_size = 24832;
        } else if (udp_profile_lidar == "LEGACY") {
            profile = LidarProfile::LEGACY;
            lidar_packet_size = 24896;
        } else {
            profile = LidarProfile::UNKNOWN;
        }

        switch (profile) {
            case LidarProfile::RNG19_RFL8_SIG16_NIR16:
#ifdef DEBUG
                std::cout << "[Main] Detected RNG19_RFL8_SIG16_NIR16 lidar udp profile." << std::endl;
#endif
                // Use default parameters or adjust if needed
                threads.emplace_back([&]() { pipeline.runOusterLidarListenerSingleReturn(ioContextPoints, config_lidar, std::vector<int>{0}); });
                break;

            case LidarProfile::LEGACY:
#ifdef DEBUG
                std::cout << "[Main] Detected LEGACY lidar udp profile." << std::endl;
#endif
                // Example: Adjust buffer size or port for LEGACY mode if needed
                // bufferSize = 16384; // Example adjustment
                threads.emplace_back([&]() { pipeline.runOusterLidarListenerLegacy(ioContextPoints, config_lidar, std::vector<int>{0}); });
                break;

            case LidarProfile::UNKNOWN:
            default:
                std::cerr << "[Main] Error: Unknown or unsupported udp_profile_lidar: " << udp_profile_lidar << std::endl;
                return EXIT_FAILURE;
        }

        threads.emplace_back([&]() { pipeline.runGNSSID20Listener(ioContextPoints, config_compass, std::vector<int>{1}); });
        threads.emplace_back([&]() { pipeline.dataAlignmentID20(std::vector<int>{2}); });
        threads.emplace_back([&]() { pipeline.runLoStateEstimation(std::vector<int>{3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18}); });
        threads.emplace_back([&]() { pipeline.runGroundTruthEstimation(gt_filename, std::vector<int>{19}); });
        threads.emplace_back([&]() { pipeline.processLogQueue(log_filename,std::vector<int>{20}); });

        while (SLAMPipeline::running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        ioContextPoints.stop();

        for (auto& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        // --- SAFE SHUTDOWN POINT ---
        // All threads have stopped. It is now 100% safe to access pipeline data.
        std::cout << "[Main] All threads joined. Saving final results..." << std::endl;
        pipeline.saveOdometryResults(timestamp); // Call the new safe method
        
    } catch (const std::exception& e) {
        std::cerr << "Error: [Main] " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::cout << "[Main] All processes stopped. Exiting program." << std::endl;
    return EXIT_SUCCESS;
}

