#include <rclcpppipeline.hpp>

using json = nlohmann::json;

std::atomic<bool> SlamPipeline::running_{true};
std::condition_variable SlamPipeline::globalCV_;

// -----------------------------------------------------------------------------

void SlamPipeline::logMessage(const std::string& level, const std::string& message) {
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

sensor_msgs::msg::PointCloud2 SlamPipeline::toPointCloud2(const std::vector<stateestimate::Point3D>& pointcloud,
                                                         const std::string& frame_id,
                                                         const rclcpp::Time& stamp) const {
    sensor_msgs::msg::PointCloud2 points_msg;
    points_msg.header.frame_id = frame_id;
    points_msg.header.stamp = stamp;
    points_msg.height = 1; // Unorganized point cloud
    points_msg.width = pointcloud.size();
    points_msg.is_bigendian = false;
    points_msg.is_dense = true; // Assume no invalid points

    // Define fields (x, y, z as float32)
    sensor_msgs::msg::PointField field_x, field_y, field_z;
    field_x.name = "x";
    field_x.offset = 0;
    field_x.datatype = sensor_msgs::msg::PointField::FLOAT32;
    field_x.count = 1;
    field_y.name = "y";
    field_y.offset = 4;
    field_y.datatype = sensor_msgs::msg::PointField::FLOAT32;
    field_y.count = 1;
    field_z.name = "z";
    field_z.offset = 8;
    field_z.datatype = sensor_msgs::msg::PointField::FLOAT32;
    field_z.count = 1;
    points_msg.fields = {field_x, field_y, field_z};
    points_msg.point_step = 12; // 3 * sizeof(float) = 12 bytes
    points_msg.row_step = points_msg.point_step * points_msg.width;

    // Serialize point cloud data
    points_msg.data.resize(points_msg.row_step);
    for (size_t i = 0; i < pointcloud.size(); ++i) {
        const auto& pt = pointcloud[i].pt; // Assume pt is Eigen::Vector3d
        float x = static_cast<float>(pt.x());
        float y = static_cast<float>(pt.y());
        float z = static_cast<float>(pt.z());
        std::memcpy(&points_msg.data[i * points_msg.point_step], &x, sizeof(float));
        std::memcpy(&points_msg.data[i * points_msg.point_step + 4], &y, sizeof(float));
        std::memcpy(&points_msg.data[i * points_msg.point_step + 8], &z, sizeof(float));
    }

    return points_msg;
}

// -----------------------------------------------------------------------------

rclcpp::Time SlamPipeline::convertToRclcppTime(double timestamp_seconds) {
    // Check for invalid or negative timestamp
    if (timestamp_seconds < 0.0) {
#ifdef DEBUG
    logMessage("LOGGING", "Invalid timestamp. Given timestamp (sec) is below 0.");
#endif
        return rclcpp::Time(0, 0, RCL_ROS_TIME);
    }

    // Split the timestamp into seconds and nanoseconds
    int64_t seconds = static_cast<int64_t>(timestamp_seconds);
    uint32_t nanoseconds = static_cast<uint32_t>((timestamp_seconds - seconds) * 1e9);

    // Create rclcpp::Time object
    return rclcpp::Time(seconds, nanoseconds, RCL_ROS_TIME);
}

// -----------------------------------------------------------------------------

SlamPipeline::SlamPipeline(const std::string& slam_registration,
                            const std::string& odom_json_path,
                            const std::string& lidar_json_path,
                            const lidarDecode::OusterLidarCallback::LidarTransformPreset& T_preset,
                            uint16_t N,
                            rclcpp::Node::SharedPtr node,
                            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr raw_points_publisher,
                            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr sampled_points_publisher,
                            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr map_points_publisher,
                            rclcpp::Publisher<nav_msgs::msg::Odometry>::SharedPtr odometry_publisher,
                            std::unique_ptr<tf2_ros::TransformBroadcaster> tf_broadcaster,
                            std::unique_ptr<tf2_ros::StaticTransformBroadcaster> tf_static_broadcaster)
    : node_(node),
      odometry_(stateestimate::Odometry::Get(slam_registration, odom_json_path)),
      lidarCallback_(lidar_json_path, T_preset, N),
      raw_points_publisher_(raw_points_publisher),
      sampled_points_publisher_(sampled_points_publisher),
      map_points_publisher_(map_points_publisher),
      odometry_publisher_(odometry_publisher),
      tf_broadcaster_(std::move(tf_broadcaster)),
      tf_static_broadcaster_(std::move(tf_static_broadcaster)) {
      odometry_->T_i_r_gt_poses.reserve(GT_SIZE_COMPASS);
#ifdef DEBUG
    logMessage("LOGGING", "SLAMPipeline and Odometry object initialized.");
#endif
}

// -----------------------------------------------------------------------------

void SlamPipeline::signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        running_.store(false, std::memory_order_release);
        globalCV_.notify_all();
    }
}

// -----------------------------------------------------------------------------

void SlamPipeline::setThreadAffinity(const std::vector<int>& coreIDs) {
    if (coreIDs.empty()) { return; }
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
    if (!validCores) { return; }

    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
        running_.store(false);
    }
}

// -----------------------------------------------------------------------------

void SlamPipeline::processLogQueue(const std::string& filename, const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);
    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        std::ostringstream oss;
        oss << "[" << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ") << "] "
            << "[ERROR] failed to open file " << filename << " for writing.\n";
        std::cerr << oss.str();
#ifdef DEBUG
        logMessage("ERROR", oss.str());
#endif
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
    outfile.flush();
    outfile.close();
}

// -----------------------------------------------------------------------------

void SlamPipeline::runOusterLidarListenerSingleReturn(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores) {
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

void SlamPipeline::runOusterLidarListenerLegacy(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores) {
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

void SlamPipeline::runGNSSID20Listener(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores) {
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
//                         if (!gnss_buffer_.push(new_frame)) {
// #ifdef DEBUG
//                             logMessage("WARNING", "GNSS Listener: SPSC ID20 buffer push failed for UnixTime " +
//                                        std::to_string(new_frame.unixTime));
// #endif
//                         }
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

void SlamPipeline::dataAlignmentID20(const std::vector<int>& allowedCores) {
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

void SlamPipeline::runLoStateEstimation(const std::vector<int>& allowedCores) {
    setThreadAffinity(allowedCores);

    // OpenMP thread limit based on allowed cores
    omp_set_num_threads(allowedCores.size());

    constexpr size_t max_empty_buffer_count = 100; // Wait if no data for too long
    size_t consecutive_empty_buffer_count = 0;

    while (running_.load(std::memory_order_acquire)) {
        try {
            // 1. Pop combined LiDAR-GNSS data from buffer
            LidarGnssWindowDataFrame tempCombineddata;
            if (!lidar_gnsswindow_buffer_.pop(tempCombineddata)) {

                // Handle buffer starvation
                if (++consecutive_empty_buffer_count >= max_empty_buffer_count) {
#ifdef DEBUG
                    logMessage("WARNING", "runLoStateEstimation: No data available after multiple attempts.");
#endif
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

#ifdef DEBUG
            // Start timer
            auto start_time = std::chrono::high_resolution_clock::now();
#endif

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
                Eigen::Matrix3d Rb2m = navMath::Cb2n(navMath::getQuat(currFrame.roll, currFrame.pitch, currFrame.yaw));
                Eigen::Matrix4d Tb2m = Eigen::Matrix4d::Identity();
                Tb2m.block<3, 3>(0, 0) = Rb2m;

                // Initialize odometry
                odometry_->initializeInitialPose(Tb2m); //body to global map
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
            auto raw_points_ts = convertToRclcppTime(currDataFrame.pointcloud.front().timestamp);
            auto raw_points_msg = toPointCloud2(currDataFrame.pointcloud, "body", raw_points_ts);
            raw_points_publisher_->publish(raw_points_msg);

            // 5. Register frame with odometry
            // const auto summary = odometry_->registerFrame(currDataFrame);

#ifdef DEBUG
            // Stop timer
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            std::ostringstream oss_timer;
            oss_timer << "runLoStateEstimation: Frame processing time: " << duration.count() << " ms.";
            logMessage("TIMER", oss_timer.str());
#endif

//             if (!summary.success) {
// #ifdef DEBUG
//                 logMessage("WARNING", "runLoStateEstimation: State estimation failed.");
// #endif
//             } else {
// #ifdef DEBUG
//                 logMessage("LOGGING", "runLoStateEstimation: Successfully registered frame with timestamp " +
//                            std::to_string(currDataFrame.timestamp));
// #endif
//             }

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

