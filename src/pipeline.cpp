#include <pipeline.hpp>

using json = nlohmann::json;

std::atomic<bool> SLAMPipeline::running_{true};
std::condition_variable SLAMPipeline::globalCV_;

// -----------------------------------------------------------------------------

void SLAMPipeline::logMessage(const std::string& level, const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << "[" << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ") << "] "
        << "[" << level << "] " << message << "\n";
    if (!log_queue_.push(oss.str())) {
        dropped_logs_.fetch_add(1, std::memory_order_relaxed);
    }
}

// -----------------------------------------------------------------------------

SLAMPipeline::SLAMPipeline(const std::string& slam_registration, const std::string& odom_json_path, const std::string& lidar_json_path, const lidarDecode::OusterLidarCallback::LidarTransformPreset& T_preset, uint16_t N) 
    : odometry_(stateestimate::Odometry::Get(slam_registration, odom_json_path)), // <-- INITIALIZE HERE, 
    lidarCallback_(lidar_json_path, T_preset, N) {// You can initialize other members here too
    temp_IMU_vec_data_.reserve(VECTOR_SIZE_IMU);
    odometry_->T_i_r_gt_poses.reserve(GT_SIZE_COMPASS);

#ifdef DEBUG
    logMessage("LOGGING", "SLAMPipeline and Odometry object initialized.");
#endif
}

// -----------------------------------------------------------------------------

void SLAMPipeline::signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        running_.store(false, std::memory_order_release);
        globalCV_.notify_all();
    }
}

// -----------------------------------------------------------------------------

void SLAMPipeline::setThreadAffinity(const std::vector<int>& coreIDs) {
    if (coreIDs.empty()) {return;}
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    const unsigned int maxCores = std::thread::hardware_concurrency();
    uint32_t validCores = 0;

    for (int coreID : coreIDs) {
        if (coreID >= 0 && static_cast<unsigned>(coreID) < maxCores) {
            CPU_SET(coreID, &cpuset);
            validCores |= (1 << coreID);
        }
    }
    if (!validCores) {
            return;
        }

    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
        running_.store(false); // Optionally terminate
    }
}

// -----------------------------------------------------------------------------

void SLAMPipeline::processLogQueue(const std::string& filename, const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores); // Pin logging thread to specified cores

    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss;
        oss << "[" << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ") << "] "
            << "[ERROR] failed to open file " << filename << " for writing.\n";
        std::cerr << oss.str(); // Fallback to cerr if file cannot be opened
        return;
    }

    std::string message;
    int lastReportedDrops = 0;
    while (running_.load(std::memory_order_acquire)) {
        if (log_queue_.pop(message)) {
            outfile << message;
            int currentDrops = dropped_logs_.load(std::memory_order_relaxed);
            if (currentDrops > lastReportedDrops && (currentDrops - lastReportedDrops) >= 100) {
                auto now = std::chrono::system_clock::now();
                auto now_time_t = std::chrono::system_clock::to_time_t(now);
                std::ostringstream oss;
                oss << "[" << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ") << "] "
                    << "[WARNING] " << (currentDrops - lastReportedDrops) << " log messages dropped due to queue overflow.\n";
                outfile << oss.str();
                lastReportedDrops = currentDrops;
            }
        } else {
            std::this_thread::yield();
        }
    }
    while (log_queue_.pop(message)) {
        outfile << message;
    }
    int finalDrops = dropped_logs_.load(std::memory_order_relaxed);
    if (finalDrops > lastReportedDrops) {

        outfile << "[LOGGING] Final report: " << (finalDrops - lastReportedDrops) << " log messages dropped.\n";
    }

    outfile.flush(); // Ensure data is written
    outfile.close();
}

// -----------------------------------------------------------------------------

void SLAMPipeline::runOusterLidarListenerSingleReturn(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);
    running_.store(true, std::memory_order_release);

    // Validate UdpSocketConfig
    if (udp_config.host.empty() || udp_config.port == 0 || udp_config.bufferSize == 0) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Invalid host, port, or buffer size. Host: " << udp_config.host 
            << ", Port: " << udp_config.port << ", Buffer: " << udp_config.bufferSize;
        logMessage("ERROR", oss.str());
#endif
        return;
    }
    if (udp_config.receiveTimeout && udp_config.receiveTimeout->count() <= 0) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Invalid receive timeout: " << udp_config.receiveTimeout->count() << " ms";
        logMessage("ERROR", oss.str());
#endif
        return;
    }
    if (udp_config.multicastGroup && !udp_config.multicastGroup->is_multicast()) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Invalid multicast group: " << udp_config.multicastGroup->to_string();
        logMessage("ERROR", oss.str());
#endif
        return;
    }

    try {
        // Data callback: Decode and push to SPSC buffer
        auto dataCallback = [this](udp_socket::SpanType packet_data) {
            try {
                lidarDecode::LidarDataFrame temp_lidar_data_;
                lidarCallback_.decode_packet_single_return(
                    std::vector<uint8_t>(packet_data.begin(), packet_data.end()), temp_lidar_data_);
                if (temp_lidar_data_.numberpoints > 0 && temp_lidar_data_.frame_id != frame_id_) {
                    frame_id_ = temp_lidar_data_.frame_id;
                    if (!lidar_buffer_.push(std::move(temp_lidar_data_))) {
#ifdef DEBUG
                        logMessage("WARNING", "Lidar Listener: SPSC buffer push failed for frame " + 
                                   std::to_string(frame_id_));
#endif
                    } else {
#ifdef DEBUG
                        std::ostringstream oss;
                        oss << "Lidar Listener: Processed frame " << frame_id_ << " with " 
                            << temp_lidar_data_.numberpoints << " points";
                        logMessage("LOGGING", oss.str());
#endif
                    }
                }
            } catch (const std::exception& e) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "Lidar Listener: Decode error: " << e.what();
                logMessage("WARNING", oss.str());
#endif
            }
        };

        // Error callback: Log errors and handle shutdown
        auto errorCallback = [this](const boost::system::error_code& ec) {
#ifdef DEBUG
            std::ostringstream oss;
            oss << "Lidar Listener: UDP error: " << ec.message() << " (value: " << ec.value() << ")";
            logMessage("WARNING", oss.str());
#endif
            if (ec == boost::asio::error::operation_aborted || !running_.load(std::memory_order_acquire)) {
                running_.store(false, std::memory_order_release);
            }
        };

        // Create UDP socket
        auto socket = udp_socket::UdpSocket::create(ioContext, udp_config, dataCallback, errorCallback);
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Started on " << udp_config.host << ":" << udp_config.port 
            << " with buffer size " << udp_config.bufferSize;
        logMessage("LOGGING", oss.str());
#endif

        // Run io_context (single-threaded, non-blocking)
        while (running_.load(std::memory_order_acquire)) {
            try {
                ioContext.run_one(); // Process one event at a time
            } catch (const std::exception& e) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "Lidar Listener: io_context exception: " << e.what();
                logMessage("WARNING", oss.str());
#endif
                if (running_.load(std::memory_order_acquire)) {
                    ioContext.restart();
#ifdef DEBUG
                    logMessage("LOGGING", "Lidar Listener: io_context restarted.");
#endif
                } else {
                    break;
                }
            }
        }

        // Cleanup
        socket->stop();
        if (!ioContext.stopped()) {
            ioContext.stop();
#ifdef DEBUG
            logMessage("LOGGING", "Lidar Listener: Stopped");
#endif
        }

    } catch (const std::exception& e) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Setup exception: " << e.what();
        logMessage("WARNING", oss.str());
#endif
        running_.store(false, std::memory_order_release);
        if (!ioContext.stopped()) {
            ioContext.stop();
#ifdef DEBUG
            logMessage("LOGGING", "Lidar Listener: Stopped");
#endif
        }
    }
}

// -----------------------------------------------------------------------------

void SLAMPipeline::runOusterLidarListenerLegacy(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);
    running_.store(true, std::memory_order_release);

    // Validate UdpSocketConfig
    if (udp_config.host.empty() || udp_config.port == 0 || udp_config.bufferSize == 0) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Invalid host, port, or buffer size. Host: " << udp_config.host 
            << ", Port: " << udp_config.port << ", Buffer: " << udp_config.bufferSize;
        logMessage("ERROR", oss.str());
#endif
        return;
    }
    if (udp_config.receiveTimeout && udp_config.receiveTimeout->count() <= 0) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Invalid receive timeout: " << udp_config.receiveTimeout->count() << " ms";
        logMessage("ERROR", oss.str());
#endif
        return;
    }
    if (udp_config.multicastGroup && !udp_config.multicastGroup->is_multicast()) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Invalid multicast group: " << udp_config.multicastGroup->to_string();
        logMessage("ERROR", oss.str());
#endif
        return;
    }

    try {
        // Data callback: Decode and push to SPSC buffer
        auto dataCallback = [this](udp_socket::SpanType packet_data) {
            try {
                lidarDecode::LidarDataFrame temp_lidar_data_;
                lidarCallback_.decode_packet_legacy(
                    std::vector<uint8_t>(packet_data.begin(), packet_data.end()), temp_lidar_data_);
                if (temp_lidar_data_.numberpoints > 0 && temp_lidar_data_.frame_id != frame_id_) {
                    frame_id_ = temp_lidar_data_.frame_id;
                    if (!lidar_buffer_.push(std::move(temp_lidar_data_))) {
#ifdef DEBUG
                        logMessage("WARNING", "Lidar Listener: SPSC buffer push failed for frame " + 
                                   std::to_string(frame_id_));
#endif
                    } else {
#ifdef DEBUG
                        std::ostringstream oss;
                        oss << "Lidar Listener: Processed frame " << frame_id_ << " with " 
                            << temp_lidar_data_.numberpoints << " points";
                        logMessage("LOGGING", oss.str());
#endif
                    }
                }
            } catch (const std::exception& e) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "Lidar Listener: Decode error: " << e.what();
                logMessage("WARNING", oss.str());
#endif
            }
        };

        // Error callback: Log errors and handle shutdown
        auto errorCallback = [this](const boost::system::error_code& ec) {
#ifdef DEBUG
            std::ostringstream oss;
            oss << "Lidar Listener: UDP error: " << ec.message() << " (value: " << ec.value() << ")";
            logMessage("WARNING", oss.str());
#endif
            if (ec == boost::asio::error::operation_aborted || !running_.load(std::memory_order_acquire)) {
                running_.store(false, std::memory_order_release);
            }
        };

        // Create UDP socket
        auto socket = udp_socket::UdpSocket::create(ioContext, udp_config, dataCallback, errorCallback);
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Started on " << udp_config.host << ":" << udp_config.port 
            << " with buffer size " << udp_config.bufferSize;
        logMessage("LOGGING", oss.str());
#endif

        // Run io_context (single-threaded, non-blocking)
        while (running_.load(std::memory_order_acquire)) {
            try {
                ioContext.run_one(); // Process one event at a time
            } catch (const std::exception& e) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "Lidar Listener: io_context exception: " << e.what();
                logMessage("WARNING", oss.str());
#endif
                if (running_.load(std::memory_order_acquire)) {
                    ioContext.restart();
#ifdef DEBUG
                    logMessage("LOGGING", "Lidar Listener: io_context restarted.");
#endif
                } else {
                    break;
                }
            }
        }

        // Cleanup
        socket->stop();
        if (!ioContext.stopped()) {
            ioContext.stop();
#ifdef DEBUG
            logMessage("LOGGING", "Lidar Listener: Stopped");
#endif
        }

    } catch (const std::exception& e) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "Lidar Listener: Setup exception: " << e.what();
        logMessage("WARNING", oss.str());
#endif
        running_.store(false, std::memory_order_release);
        if (!ioContext.stopped()) {
            ioContext.stop();
#ifdef DEBUG
            logMessage("LOGGING", "Lidar Listener: Stopped");
#endif
        }
    }
}

// -----------------------------------------------------------------------------

void SLAMPipeline::runGNSSID20Listener(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);
    running_.store(true, std::memory_order_release);

    // Validate UdpSocketConfig
    if (udp_config.host.empty() || udp_config.port == 0 || udp_config.bufferSize == 0) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "GNSS Listener: Invalid host, port, or buffer size. Host: " << udp_config.host
            << ", Port: " << udp_config.port << ", Buffer: " << udp_config.bufferSize;
        logMessage("ERROR", oss.str());
#endif
        return;
    }
    if (udp_config.receiveTimeout && udp_config.receiveTimeout->count() <= 0) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "GNSS Listener: Invalid receive timeout: " << udp_config.receiveTimeout->count() << " ms";
        logMessage("ERROR", oss.str());
#endif
        return;
    }
    if (udp_config.multicastGroup && !udp_config.multicastGroup->is_multicast()) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "GNSS Listener: Invalid multicast group: " << udp_config.multicastGroup->to_string();
        logMessage("ERROR", oss.str());
#endif
        return;
    }

    try {
        // Data callback: Decode and process GNSS frames
        auto dataCallback = [this](udp_socket::SpanType packet_data) {
            try {
                decodeNav::DataFrameID20 new_frame;
                gnssCallback_.decode_ID20(std::vector<uint8_t>(packet_data.begin(), packet_data.end()), new_frame);
                if (new_frame.unixTime > 0 && new_frame.unixTime != this->unixTime) {
                    this->unixTime = new_frame.unixTime;
                    // Maintain sliding window
                    if (gnss_data_window_.size() >= DATA_SIZE_GNSS) {
                        gnss_data_window_.pop_front();
                    }
                    gnss_data_window_.push_back(new_frame);
                    if (gnss_data_window_.size() == DATA_SIZE_GNSS) {
                        if (!gnss_window_buffer_.push(gnss_data_window_)) {
#ifdef DEBUG
                            logMessage("WARNING", "GNSS Listener: SPSC ID20 Vec buffer push failed for UnixTime " +
                                       std::to_string(new_frame.unixTime));
#endif
                        }
                        if (!gnss_buffer_.push(new_frame)) {
#ifdef DEBUG
                            logMessage("WARNING", "GNSS Listener: SPSC ID20 buffer push failed for UnixTime " +
                                       std::to_string(new_frame.unixTime));
#endif
                        }
                    }
#ifdef DEBUG
                    std::ostringstream oss;
                    oss << "GNSS Listener: Processed frame. UnixTime: " << new_frame.unixTime
                        << ", Latitude: " << new_frame.latitude << ", Longitude: " << new_frame.longitude
                        << ", Altitude: " << new_frame.altitude;
                    logMessage("LOGGING", oss.str());
#endif
                }
            } catch (const std::exception& e) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "GNSS Listener: Decode error: " << e.what();
                logMessage("WARNING", oss.str());
#endif
            }
        };

        // Error callback: Log errors and handle shutdown
        auto errorCallback = [this](const boost::system::error_code& ec) {
#ifdef DEBUG
            std::ostringstream oss;
            oss << "GNSS Listener: UDP error: " << ec.message() << " (value: " << ec.value() << ")";
            logMessage("WARNING", oss.str());
#endif
            if (ec == boost::asio::error::operation_aborted || !running_.load(std::memory_order_acquire)) {
                running_.store(false, std::memory_order_release);
            }
        };

        // Create UDP socket
        auto socket = udp_socket::UdpSocket::create(ioContext, udp_config, dataCallback, errorCallback);
#ifdef DEBUG
        std::ostringstream oss;
        oss << "GNSS Listener: Started on " << udp_config.host << ":" << udp_config.port
            << " with buffer size " << udp_config.bufferSize;
        logMessage("LOGGING", oss.str());
#endif

        // Run io_context (single-threaded, non-blocking)
        while (running_.load(std::memory_order_acquire)) {
            try {
                ioContext.run_one(); // Process one event at a time
            } catch (const std::exception& e) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "GNSS Listener: io_context exception: " << e.what();
                logMessage("WARNING", oss.str());
#endif
                if (running_.load(std::memory_order_acquire)) {
                    ioContext.restart();
#ifdef DEBUG
                    logMessage("LOGGING", "GNSS Listener: io_context restarted.");
#endif
                } else {
                    break;
                }
            }
        }

        // Cleanup
        socket->stop();
        if (!ioContext.stopped()) {
            ioContext.stop();
#ifdef DEBUG
            logMessage("LOGGING", "GNSS Listener: Stopped");
#endif
        }

    } catch (const std::exception& e) {
#ifdef DEBUG
        std::ostringstream oss;
        oss << "GNSS Listener: Setup exception: " << e.what();
        logMessage("WARNING", oss.str());
#endif
        running_.store(false, std::memory_order_release);
        if (!ioContext.stopped()) {
            ioContext.stop();
#ifdef DEBUG
            logMessage("LOGGING", "GNSS Listener: Stopped");
#endif
        }
    }
}

// -----------------------------------------------------------------------------

void SLAMPipeline::dataAlignmentID20(const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);

    constexpr size_t max_empty_buffer_count = 100; // Wait if no data for too long

    while (running_.load(std::memory_order_acquire)) {
        try {
            // 1. Pop a LiDAR frame from the buffer
            lidarDecode::LidarDataFrame lidar_frame;
            size_t consecutive_empty_buffer_count = 0;
            if (!lidar_buffer_.pop(lidar_frame) || lidar_frame.timestamp_points.empty()) {

                // Increment empty buffer counter and wait briefly
                if (++consecutive_empty_buffer_count >= max_empty_buffer_count) {
#ifdef DEBUG
                    logMessage("WARNING", "dataAlignmentID20: No LiDAR data available after multiple attempts.");
#endif
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            consecutive_empty_buffer_count = 0; // Reset on successful pop

            // 2. Validate and get the time range for the LiDAR scan
            if (lidar_frame.timestamp_points.size() < 2) {
#ifdef DEBUG
                logMessage("ERROR", "dataAlignmentID20: LidarDataFrame has insufficient timestamp points.");
#endif
                continue;
            }
            const double min_lidar_time = lidar_frame.timestamp_points.front();
            const double max_lidar_time = lidar_frame.timestamp_points.back();
            if (min_lidar_time > max_lidar_time) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "dataAlignmentID20: Invalid LiDAR timestamp range (min: " << min_lidar_time
                    << ", max: " << max_lidar_time << ").";
                logMessage("ERROR", oss.str());
#endif
                continue;
            }

            // 3. Find a corresponding GNSS window
            bool aligned = false;
            while (!aligned && gnss_window_buffer_.read_available() > 0) {
                std::deque<decodeNav::DataFrameID20> gnss_window_packet;
                if (!gnss_window_buffer_.pop(gnss_window_packet) || gnss_window_packet.empty()) {

                    break; // Try next LiDAR frame
                }

                if (gnss_window_packet.size() < 2) {
#ifdef DEBUG
                    logMessage("WARNING", "dataAlignmentID20: GNSS window packet has insufficient data points.");
#endif
                    continue;
                }

                const double min_gnss_time = gnss_window_packet.front().unixTime;
                const double max_gnss_time = gnss_window_packet.back().unixTime;
                if (min_gnss_time > max_gnss_time) {
#ifdef DEBUG
                    std::ostringstream oss;
                    oss << "dataAlignmentID20: Invalid GNSS timestamp range (min: " << min_gnss_time
                        << ", max: " << max_gnss_time << ").";
                    logMessage("WARNING", oss.str());
#endif
                    continue;
                }

                // Core Alignment Logic
                if (min_lidar_time >= min_gnss_time && max_lidar_time <= max_gnss_time) {
                    // Case 1: GNSS packet envelops LiDAR frame
                    aligned = true;
#ifdef DEBUG
                    logMessage("LOGGING", "dataAlignmentID20: Found alignment envelope.");
#endif
                    std::vector<decodeNav::DataFrameID20> filtered_gnss_window_packet;
                    std::copy_if(gnss_window_packet.begin(), gnss_window_packet.end(),
                                 std::back_inserter(filtered_gnss_window_packet),
                                 [&](const decodeNav::DataFrameID20& data) {
                                     return data.unixTime >= min_lidar_time && data.unixTime <= max_lidar_time;
                                 });

                    if (!filtered_gnss_window_packet.empty()) {
                        LidarGnssWindowDataFrame combined_data;
                        combined_data.Lidar = std::move(lidar_frame);
                        combined_data.GnssWindow = std::move(filtered_gnss_window_packet);
#ifdef DEBUG
                        std::ostringstream oss1, oss2;
                        oss1 << std::fixed << std::setprecision(12);
                        oss1 << "dataAlignmentID20: LiDAR timestamp start: " << combined_data.Lidar.timestamp
                             << ", timestamp end: " << combined_data.Lidar.timestamp_end;
                        logMessage("LOGGING", oss1.str());
                        oss2 << std::fixed << std::setprecision(12);
                        oss2 << "dataAlignmentID20: GNSS Window timestamp start: " << combined_data.GnssWindow.front().unixTime
                             << ", timestamp end: " << combined_data.GnssWindow.back().unixTime;
                        logMessage("LOGGING", oss2.str());
#endif
                        if (!lidar_gnsswindow_buffer_.push(std::move(combined_data))) {
#ifdef DEBUG
                            logMessage("WARNING", "dataAlignmentID20: Failed to push combined data to buffer.");
#endif
                        } else {
#ifdef DEBUG
                            logMessage("LOGGING", "dataAlignmentID20: Successfully pushed combined data to buffer.");
#endif
                        }
                    } else {
#ifdef DEBUG
                        logMessage("WARNING", "dataAlignmentID20: Alignment envelope found, but no GNSS points within LiDAR time span.");
#endif
                    }
                } else if (min_lidar_time > min_gnss_time && max_lidar_time > max_gnss_time) {
                    // Case 2: GNSS packet is too old
                    continue;
                } else {
                    // Case 3: LiDAR frame is too old
#ifdef DEBUG
                    logMessage("ERROR", "dataAlignmentID20: LiDAR frame is too old. Discarding LiDAR frame.");
#endif
                    break;
                }
            }

        } catch (const std::exception& e) {
#ifdef DEBUG
            std::ostringstream oss;
            oss << "dataAlignmentID20: Exception occurred: " << e.what();
            logMessage("ERROR", oss.str());
#endif
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

#ifdef DEBUG
    logMessage("LOGGING", "dataAlignmentID20: Stopped");
#endif
}

// -----------------------------------------------------------------------------

void SLAMPipeline::runLoStateEstimation(const std::vector<int>& allowedCores)
{
    setThreadAffinity(allowedCores);

    // OpenMP thread limit based on allowed cores
    omp_set_num_threads(allowedCores.size());

    constexpr size_t max_empty_buffer_count = 100; // Wait if no data for too long
    size_t consecutive_empty_buffer_count = 0;

    while (running_.load(std::memory_order_acquire)) {
        try {
            //0 timer
            std::vector<std::pair<std::string, std::unique_ptr<stateestimate::Stopwatch<>>>> timer;
            // Add timers for different ICP phases (only if debug_print is true)
            timer.emplace_back("Loading ......................... ", std::make_unique<stateestimate::Stopwatch<>>(false));
            timer.emplace_back("Registration .................... ", std::make_unique<stateestimate::Stopwatch<>>(false));

            timer[0].second->start();
            // 1. Pop combined LiDAR-GNSS data from buffer
            LidarGnssWindowDataFrame tempCombineddata;
            if (!lidar_gnsswindow_buffer_.pop(tempCombineddata)) {

                // Handle buffer starvation
                if (++consecutive_empty_buffer_count >= max_empty_buffer_count) {
#ifdef DEBUG
                    logMessage("WARNING", "runLoStateEstimation: No data available after multiple attempts.");
#endif
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            consecutive_empty_buffer_count = 0; // Reset on successful pop

            // 2. Validate data
            if (tempCombineddata.Lidar.timestamp_points.empty() || tempCombineddata.GnssWindow.empty()) {
#ifdef DEBUG
                logMessage("ERROR", "runLoStateEstimation: Empty LiDAR or GNSS data in frame.");
#endif
                continue;
            }

            // 3. Initialize odometry if needed
            if (!init_) {
                if (tempCombineddata.GnssWindow.empty()) {
#ifdef DEBUG
                    logMessage("ERROR", "runLoStateEstimation: Cannot initialize with empty GNSS window.");
#endif
                    continue;
                }
                decodeNav::DataFrameID20 currFrame = tempCombineddata.GnssWindow.back();
                if (currFrame.unixTime <= 0) {
#ifdef DEBUG
                    logMessage("ERROR", "runLoStateEstimation: Invalid GNSS timestamp for initialization.");
#endif
                    continue;
                }

                // Calculate rotation from robot to map (R_mr)
                Eigen::Matrix3d Rb2w = navMath::Cb2n(navMath::getQuat(currFrame.roll, currFrame.pitch, currFrame.yaw));
                Eigen::Matrix4d Tb2w = Eigen::Matrix4d::Identity();
                Tb2w.block<3, 3>(0, 0) = Rb2w;

                // Initialize odometry
                odometry_->initializeInitialPose(Tb2w); //body to global map
                init_ = true;

#ifdef DEBUG
                std::ostringstream oss;
                oss << "runLoStateEstimation (ORIGIN): " << steam::traj::Time(currFrame.unixTime).nanosecs() << " "
                    << currFrame.latitude << " " << currFrame.longitude << " " << currFrame.altitude
                    << " " << currFrame.roll << " " << currFrame.pitch << " " << currFrame.yaw;
                logMessage("LOGGING", oss.str());
#endif
            }

            // 4. Convert data to stateestimate::DataFrame
            stateestimate::DataFrame currDataFrame;
            std::vector<lidarDecode::Point3D> tempLidarframe = tempCombineddata.Lidar.toPoint3D();
            currDataFrame.timestamp = tempCombineddata.Lidar.timestamp;

            // Parallel processing with OpenMP
#pragma omp parallel sections
            {
#pragma omp section
                {
                    // Task 1: Parallel LiDAR point cloud conversion
                    currDataFrame.pointcloud.resize(tempLidarframe.size());
#pragma omp parallel for
                    for (size_t i = 0; i < tempLidarframe.size(); ++i) {
                        currDataFrame.pointcloud[i].raw_pt = tempLidarframe[i].raw_pt;
                        currDataFrame.pointcloud[i].pt = tempLidarframe[i].pt;
                        currDataFrame.pointcloud[i].radial_velocity = tempLidarframe[i].radial_velocity;
                        currDataFrame.pointcloud[i].alpha_timestamp = tempLidarframe[i].alpha_timestamp;
                        currDataFrame.pointcloud[i].timestamp = tempLidarframe[i].timestamp;
                        currDataFrame.pointcloud[i].beam_id = tempLidarframe[i].beam_id;
                    }
                }
#pragma omp section
                {
                    // Task 2: Sequential GNSS/IMU data conversion
                    const auto& gnssWindowsource = tempCombineddata.GnssWindow;
                    currDataFrame.imu_data_vec.resize(gnssWindowsource.size());
                    for (size_t i = 0; i < gnssWindowsource.size(); ++i) {
                        currDataFrame.imu_data_vec[i].lin_acc = Eigen::Vector3d(
                            static_cast<double>(gnssWindowsource[i].accelX),
                            static_cast<double>(gnssWindowsource[i].accelY),
                            static_cast<double>(gnssWindowsource[i].accelZ)
                        );
                        currDataFrame.imu_data_vec[i].ang_vel = Eigen::Vector3d(
                            static_cast<double>(gnssWindowsource[i].angularVelocityX),
                            static_cast<double>(gnssWindowsource[i].angularVelocityY),
                            static_cast<double>(gnssWindowsource[i].angularVelocityZ)
                        );
                        currDataFrame.imu_data_vec[i].timestamp = gnssWindowsource[i].unixTime;
                    }
                }
            }
            timer[0].second->stop();

            // 5. Register frame with odometry
            timer[1].second->start();
            const auto summary = odometry_->registerFrame(currDataFrame);
            timer[1].second->stop();

            logMessage("LOGGING", "runLoStateEstimation: INNER LOOP TIMER.");
            for (size_t i = 0; i < timer.size(); i++) {
                std::ostringstream oss;
                oss << "Elapsed: " << timer[i].first << *(timer[i].second);
                logMessage("LOGGING", oss.str());
            }

            if (!summary.success) {
#ifdef DEBUG
                logMessage("WARNING", "runLoStateEstimation: State estimation failed.");
                
#endif
                continue;
            } else {
#ifdef DEBUG
                logMessage("LOGGING", "runLoStateEstimation: Successfully registered frame with timestamp " +
                           std::to_string(currDataFrame.timestamp));
#endif
            }

            // if summary success, send data for vizualization
            //###############!!!!!!!!!!!!!!!!!!! CRITICAL
            //as sensor to body Tb2s is defined identity, (only in this case)
            
            // Eigen::Matrix4d Tb2m = Eigen::Matrix4d::Identity();
            // Tb2m.block<3, 3>(0, 0) = summary.Rs2m;
            // Tb2m.block<3, 1>(0, 3) = summary.ts2m;

            // auto &sampled_points = summary.corrected_points;
            // auto map_points = odometry_->map();
            

        } catch (const std::exception& e) {
#ifdef DEBUG
            std::ostringstream oss;
            oss << "runLoStateEstimation: Exception occurred: " << e.what();
            logMessage("ERROR", oss.str());
#endif
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

#ifdef DEBUG
    logMessage("LOGGING", "runLoStateEstimation: Stopped");
#endif
}

// -----------------------------------------------------------------------------

void SLAMPipeline::runGroundTruthEstimation(const std::string& filename, const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);

    // Open output file
    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss;
        oss << "[" << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ") << "] "
            << "[ERROR] failed to open file " << filename << " for writing.";
        std::cerr << oss.str();
#ifdef DEBUG
        logMessage("ERROR", oss.str());
#endif
        return;
    }

    constexpr size_t max_empty_buffer_count = 100; // Wait if no data for too long
    size_t consecutive_empty_buffer_count = 0;

    while (running_.load(std::memory_order_acquire)) {
        try {
            // Pop GNSS frame from buffer
            decodeNav::DataFrameID20 currFrame;
            if (!gnss_buffer_.pop(currFrame)) {

                // Handle buffer starvation
                if (++consecutive_empty_buffer_count >= max_empty_buffer_count) {
#ifdef DEBUG
                    logMessage("WARNING", "runGroundTruthEstimation: No GNSS data available after multiple attempts.");
#endif
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            consecutive_empty_buffer_count = 0; // Reset on successful pop

            // Validate GNSS data
            if (currFrame.unixTime <= 0) {
#ifdef DEBUG
                std::ostringstream oss;
                oss << "runGroundTruthEstimation: Invalid GNSS timestamp: " << currFrame.unixTime;
                logMessage("ERROR", oss.str());
#endif
                continue;
            }

            // Process frame
            Eigen::Matrix4d Tb2w = Eigen::Matrix4d::Identity();
            if (is_firstFrame_) {
                // Handle the first frame
                Eigen::Matrix3d Rb2w = navMath::Cb2n(navMath::getQuat(
                    currFrame.roll, currFrame.pitch, currFrame.yaw));
                // Eigen::Matrix3d Rm2b = Rb2m.transpose();
                Tb2w.block<3, 3>(0, 0) = Rb2w;

                originFrame_ = currFrame;
                is_firstFrame_ = false;

                // Output origin LLA and transformation
                steam::traj::Time Time(currFrame.unixTime);
                outfile << std::fixed << std::setprecision(12) << Time.nanosecs() << " "
                        << currFrame.latitude << " " << currFrame.longitude << " " << currFrame.altitude << " "
                        << currFrame.roll << " " << currFrame.pitch << " " << currFrame.yaw << "\n";
                outfile << std::fixed << std::setprecision(12) << Time.nanosecs() << " "
                        << Tb2w(0, 0) << " " << Tb2w(0, 1) << " " << Tb2w(0, 2) << " " << Tb2w(0, 3) << " "
                        << Tb2w(1, 0) << " " << Tb2w(1, 1) << " " << Tb2w(1, 2) << " " << Tb2w(1, 3) << " "
                        << Tb2w(2, 0) << " " << Tb2w(2, 1) << " " << Tb2w(2, 2) << " " << Tb2w(2, 3) << " "
                        << Tb2w(3, 0) << " " << Tb2w(3, 1) << " " << Tb2w(3, 2) << " " << Tb2w(3, 3) << "\n";

#ifdef DEBUG
                std::ostringstream oss;
                oss << "runGroundTruthEstimation (ORIGIN): " << Time.nanosecs() << " "
                    << currFrame.latitude << " " << currFrame.longitude << " " << currFrame.altitude << " "
                    << currFrame.roll << " " << currFrame.pitch << " " << currFrame.yaw;
                logMessage("LOGGING", oss.str());
#endif
            } else {
                // Handle subsequent frames
                Eigen::Matrix3d Rb2w = navMath::Cb2n(navMath::getQuat(
                    currFrame.roll, currFrame.pitch, currFrame.yaw));
                Eigen::Vector3d tb2w = navMath::LLA2NED(
                    currFrame.latitude, currFrame.longitude, currFrame.altitude,
                    originFrame_.latitude, originFrame_.longitude, originFrame_.altitude);
                Eigen::Matrix3d Rw2b = Rb2w.transpose();
                Eigen::Vector3d tw2b = -Rw2b * tb2w;
                Tb2w.block<3, 3>(0, 0) = Rb2w;
                Tb2w.block<3, 1>(0, 3) = tb2w;

                Eigen::Matrix4d Tw2b = Eigen::Matrix4d::Identity();
                Tw2b.block<3, 3>(0, 0) = Rw2b;
                Tw2b.block<3, 1>(0, 3) = tw2b;

                odometry_->T_i_r_gt_poses.push_back(Tw2b);

                // Output transformation
                steam::traj::Time Time(currFrame.unixTime);
                outfile << std::fixed << std::setprecision(12) << Time.nanosecs() << " "
                        << Tw2b(0, 0) << " " << Tw2b(0, 1) << " " << Tw2b(0, 2) << " " << Tw2b(0, 3) << " "
                        << Tw2b(1, 0) << " " << Tw2b(1, 1) << " " << Tw2b(1, 2) << " " << Tw2b(1, 3) << " "
                        << Tw2b(2, 0) << " " << Tw2b(2, 1) << " " << Tw2b(2, 2) << " " << Tw2b(2, 3) << " "
                        << Tw2b(3, 0) << " " << Tw2b(3, 1) << " " << Tw2b(3, 2) << " " << Tw2b(3, 3) << "\n";

#ifdef DEBUG
                std::ostringstream oss;
                oss << "runGroundTruthEstimation: Registered pose at timestamp " << Time.nanosecs();
                logMessage("LOGGING", oss.str());
#endif
            }

            // Periodic flush to ensure data is written
            if (outfile.tellp() > 1024 * 1024) { // Flush every ~1MB
                outfile.flush();
                if (!outfile.good()) {
#ifdef DEBUG
                    logMessage("ERROR", "runGroundTruthEstimation: Failed to write to file " + filename);
#endif
                    running_.store(false, std::memory_order_release);
                    break;
                }
            }

        } catch (const std::exception& e) {
#ifdef DEBUG
            std::ostringstream oss;
            oss << "runGroundTruthEstimation: Exception occurred: " << e.what();
            logMessage("ERROR", oss.str());
#endif
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    // Final flush and close
    outfile.flush();
    if (!outfile.good()) {
#ifdef DEBUG
        logMessage("ERROR", "runGroundTruthEstimation: Failed to write final data to file " + filename);
#endif
    }
    outfile.close();
#ifdef DEBUG
    logMessage("LOGGING", "runGroundTruthEstimation: Stopped");
#endif
}

// -----------------------------------------------------------------------------

void SLAMPipeline::saveOdometryResults(const std::string& timestamp) {
    if (odometry_) {
        odometry_->getResults(timestamp);
    }
}