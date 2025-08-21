#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <tf2_ros/transform_broadcaster.h>
#include <tf2_ros/static_transform_broadcaster.h>
#include <chrono>
#include <sstream>
#include <fstream>
#include <nlohmann/json.hpp>
#include <rclcpppipeline.hpp>

class SlamPipelineNode : public rclcpp::Node {
    public:
        SlamPipelineNode() : Node("slam_pipeline_node") {
            // Initialize publishers and broadcasters
            raw_points_publisher_ = create_publisher<sensor_msgs::msg::PointCloud2>("/slam_raw_points", 2);
            sampled_points_publisher_ = create_publisher<sensor_msgs::msg::PointCloud2>("/slam_sampled_points", 2);
            map_points_publisher_ = create_publisher<sensor_msgs::msg::PointCloud2>("/slam_map_points", 2);
            odometry_publisher_ = create_publisher<nav_msgs::msg::Odometry>("/slam_odometry", 10);
            tf_broadcaster_ = std::make_unique<tf2_ros::TransformBroadcaster>(*this);
            tf_static_broadcaster_ = std::make_unique<tf2_ros::StaticTransformBroadcaster>(*this);

            // Generate timestamp for filenames
            auto now = std::chrono::system_clock::now();
            auto utc_time = std::chrono::system_clock::to_time_t(now);
            std::stringstream ss;
            ss << std::put_time(std::gmtime(&utc_time), "%Y%m%d_%H%M%S");
            timestamp_ = ss.str();
        }

        void init() {
            // JSON parsing
            std::string lidar_json = "./src/slam_pipeline_ros2/config/2025047_1054_OS-2-128_122446000745.json";
            std::string config_json = "./src/slam_pipeline_ros2/config/odom_config.json";
            uint32_t lidar_packet_size = 24896;
            uint16_t udp_port_gnss = 6597;
            uint32_t id20_packet_size = 105;
            std::string udp_profile_lidar, udp_dest;

            try {
                std::ifstream json_file(lidar_json);
                if (!json_file.is_open()) {
                    throw std::runtime_error("[SlamPipelineNode] Error: Could not open JSON file: " + lidar_json);
                }
                nlohmann::json metadata_;
                json_file >> metadata_;
                json_file.close();
                if (!metadata_.contains("lidar_data_format") || !metadata_["lidar_data_format"].is_object() ||
                    !metadata_.contains("config_params") || !metadata_["config_params"].is_object() ||
                    !metadata_.contains("beam_intrinsics") || !metadata_["beam_intrinsics"].is_object() ||
                    !metadata_.contains("lidar_intrinsics") || !metadata_["lidar_intrinsics"].is_object() ||
                    !metadata_["lidar_intrinsics"].contains("lidar_to_sensor_transform")) {
                    throw std::runtime_error("[SlamPipelineNode] Invalid JSON structure");
                }
                udp_profile_lidar = metadata_["config_params"]["udp_profile_lidar"].get<std::string>();
                udp_port_lidar_ = metadata_["config_params"]["udp_port_lidar"].get<uint16_t>();
                udp_dest = metadata_["config_params"]["udp_dest"].get<std::string>();
            } catch (const std::exception& e) {
                RCLCPP_ERROR(this->get_logger(), "Error parsing JSON: %s", e.what());
                throw;
            }

            // Configure UDP sockets
            config_lidar_.host = "192.168.75.10";
            config_lidar_.port = udp_port_lidar_;
            config_lidar_.bufferSize = lidar_packet_size;
            config_lidar_.receiveTimeout = std::chrono::milliseconds(1000);
            config_lidar_.enableBroadcast = false;
            config_lidar_.multicastGroup = std::nullopt;
            config_lidar_.ttl = std::nullopt;
            config_lidar_.reuseAddress = true;

            config_compass_.host = "192.168.75.10";
            config_compass_.port = udp_port_gnss;
            config_compass_.bufferSize = id20_packet_size;
            config_compass_.receiveTimeout = std::chrono::milliseconds(1000);
            config_compass_.enableBroadcast = false;
            config_compass_.multicastGroup = std::nullopt;
            config_compass_.ttl = std::nullopt;
            config_compass_.reuseAddress = true;

            // Initialize io_context
            io_context_ = std::make_unique<boost::asio::io_context>();

            // Initialize pipeline
            pipeline_ = std::make_unique<SlamPipeline>(
                "SLAM_LIDAR_ODOM",
                config_json,
                lidar_json,
                lidarDecode::OusterLidarCallback::LidarTransformPreset::GEHLSDORF20250410,
                4,
                shared_from_this(), // Safe to call here
                odometry_publisher_,
                raw_points_publisher_,
                sampled_points_publisher_,
                map_points_publisher_,
                std::move(tf_broadcaster_),
                std::move(tf_static_broadcaster_));

            // Update lidar_packet_size based on profile
            std::string log_filename = "./src/slam_pipeline_ros2/report/log/log_report_" + timestamp_ + ".txt";
            if (udp_profile_lidar == "RNG19_RFL8_SIG16_NIR16") {
                config_lidar_.bufferSize = 24832;
                threads_.emplace_back([&]() { pipeline_->runOusterLidarListenerSingleReturn(*io_context_, config_lidar_, {0}); });
            } else if (udp_profile_lidar == "LEGACY") {
                config_lidar_.bufferSize = 24896;
                threads_.emplace_back([&]() { pipeline_->runOusterLidarListenerLegacy(*io_context_, config_lidar_, {0}); });
            } else {
                RCLCPP_ERROR(this->get_logger(), "Unknown udp_profile_lidar: %s", udp_profile_lidar.c_str());
                throw std::runtime_error("Unsupported lidar profile");
            }

            // Spawn threads
            threads_.emplace_back([&]() { pipeline_->runGNSSID20Listener(*io_context_, config_compass_, {1}); });
            threads_.emplace_back([&]() { pipeline_->dataAlignmentID20({2}); });
            threads_.emplace_back([&]() { pipeline_->runLoStateEstimation({3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}); });
            threads_.emplace_back([&]() { pipeline_->processLogQueue(log_filename, {20}); });
        }

        ~SlamPipelineNode() {
            SlamPipeline::running_.store(false, std::memory_order_release);
            SlamPipeline::globalCV_.notify_all();
            if (io_context_) {
                io_context_->stop();
            }
            for (auto& thread : threads_) {
                if (thread.joinable()) {
                    thread.join();
                }
            }
            RCLCPP_INFO(this->get_logger(), "All threads joined. Saved odometry results.");
        }

    private:
        std::unique_ptr<SlamPipeline> pipeline_;
        std::unique_ptr<boost::asio::io_context> io_context_;
        std::vector<std::thread> threads_;
        std::string timestamp_;
        rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr raw_points_publisher_;
        rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr sampled_points_publisher_;
        rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr map_points_publisher_;
        rclcpp::Publisher<nav_msgs::msg::Odometry>::SharedPtr odometry_publisher_;
        std::unique_ptr<tf2_ros::TransformBroadcaster> tf_broadcaster_;
        std::unique_ptr<tf2_ros::StaticTransformBroadcaster> tf_static_broadcaster_;
        udp_socket::UdpSocketConfig config_lidar_, config_compass_;
        uint16_t udp_port_lidar_;
};

int main(int argc, char** argv) {
    rclcpp::init(argc, argv);
    auto node = std::make_shared<SlamPipelineNode>();
    node->init(); // Initialize after construction
    rclcpp::executors::MultiThreadedExecutor executor;
    executor.add_node(node);
    executor.spin();
    rclcpp::shutdown();
    return 0;
}