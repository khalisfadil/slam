#pragma once

#include <boost/asio.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <nlohmann/json.hpp>

#include <memory>
#include <thread>
#include <iostream>
#include <fstream>
#include <chrono>
#include <Eigen/Dense>

#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <tf2_ros/transform_broadcaster.h>
#include <tf2_ros/static_transform_broadcaster.h>

#include <udpsocket.hpp>
#include <LidarDataframe.hpp>
#include <LidarIMUDataFrame.hpp>
#include <OusterLidarCallback.hpp>
#include <callback_gnssComp.hpp>
#include <DataFrame_ID20.hpp>

#include <odometry/steam_lo.hpp>
#include <utils/stopwatch.hpp>

#include <navMath.hpp>

class SlamPipeline {
    struct LidarIMUVecDataFrame {
        std::vector<lidarDecode::LidarIMUDataFrame> IMUVec;
        lidarDecode::LidarDataFrame Lidar;
    };

    struct LidarGnssWindowDataFrame {
        std::vector<decodeNav::DataFrameID20> GnssWindow;
        lidarDecode::LidarDataFrame Lidar;
    };

    public:
        static std::atomic<bool> running_;
        static std::condition_variable globalCV_;
        static std::mutex global_mutex_;
        std::atomic<int> dropped_logs_;
        boost::lockfree::spsc_queue<lidarDecode::LidarDataFrame, boost::lockfree::capacity<16>> lidar_buffer_;
        boost::lockfree::spsc_queue<std::vector<lidarDecode::LidarIMUDataFrame>, boost::lockfree::capacity<16>> imu_vec_buffer_;
        boost::lockfree::spsc_queue<lidarDecode::LidarIMUDataFrame, boost::lockfree::capacity<16>> imu_buffer_;
        boost::lockfree::spsc_queue<std::deque<decodeNav::DataFrameID20>, boost::lockfree::capacity<16>> gnss_window_buffer_;
        boost::lockfree::spsc_queue<decodeNav::DataFrameID20, boost::lockfree::capacity<16>> gnss_buffer_;
        boost::lockfree::spsc_queue<LidarIMUVecDataFrame, boost::lockfree::capacity<16>> lidar_imu_buffer_;
        boost::lockfree::spsc_queue<LidarGnssWindowDataFrame, boost::lockfree::capacity<8192>> lidar_gnsswindow_buffer_;
        boost::lockfree::spsc_queue<std::string, boost::lockfree::capacity<16>> log_queue_;

        // Constructor with ROS2 node
        explicit SlamPipeline(const std::string& slam_registration, const std::string& odom_json_path,
                            const std::string& lidar_json_path,
                            const lidarDecode::OusterLidarCallback::LidarTransformPreset& T_preset,
                            uint16_t N = 1, 
                            rclcpp::Node::SharedPtr node = nullptr,
                            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr raw_points_publisher,
                            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr sampled_points_publisher,
                            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr map_points_publisher,
                            rclcpp::Publisher<nav_msgs::msg::Odometry>::SharedPtr odometry_publisher,
                            std::unique_ptr<tf2_ros::TransformBroadcaster> tf_broadcaster,
                            std::unique_ptr<tf2_ros::StaticTransformBroadcaster> tf_static_broadcaster);
        static void signalHandler(int signal);
        void setThreadAffinity(const std::vector<int>& coreIDs);

        // Application listeners
        void runOusterLidarListenerSingleReturn(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config,
                                            const std::vector<int>& allowedCores);
        void runOusterLidarListenerLegacy(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config,
                                        const std::vector<int>& allowedCores);
        void runGNSSID20Listener(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config,
                                const std::vector<int>& allowedCores);

        // Application for logging
        void processLogQueue(const std::string& filename, const std::vector<int>& allowedCores);
        void logMessage(const std::string& level, const std::string& message);

        // Application for SLAM
        void dataAlignmentID20(const std::vector<int>& allowedCores);
        void runLoStateEstimation(const std::vector<int>& allowedCores);

        // ROS2 application
        sensor_msgs::msg::PointCloud2 toPointCloud2(const std::vector<stateestimate::Point3D>& pointcloud, const std::string& frame_id = "map", 
                                                    const rclcpp::Time& stamp = rclcpp::Clock().now()) const;
        rclcpp::Time convertToRclcppTime(double timestamp_seconds);

    private:
            
        // ROS2 node and publishers
        rclcpp::Node::SharedPtr node_;

        rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr raw_points_publisher_;
        rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr sampled_points_publisher_;
        rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr map_points_publisher_;

        rclcpp::Publisher<nav_msgs::msg::Odometry>::SharedPtr odometry_publisher_;
        std::unique_ptr<tf2_ros::TransformBroadcaster> tf_broadcaster_;
        std::unique_ptr<tf2_ros::StaticTransformBroadcaster> tf_static_broadcaster_;

        // runGroundTruthEstimation
        stateestimate::Odometry::Ptr odometry_;
        const size_t GT_SIZE_COMPASS = 120000;
        bool is_firstFrame_ = true;
        decodeNav::DataFrameID20 originFrame_;
        Eigen::Matrix3d prev_Rb2m_;

        // runLioStateEstimation
        bool init_ = false;

        // runOusterLidarListener
        lidarDecode::OusterLidarCallback lidarCallback_;
        uint16_t frame_id_ = 0;

        // runGNSSListener
        decodeNav::GnssCompassCallback gnssCallback_;
        std::deque<decodeNav::DataFrameID20> gnss_data_window_;
        double unixTime = 0.0;
        const size_t DATA_SIZE_GNSS = 15;

        // runOusterLidarIMUListener
        uint64_t Normalized_Timestamp_s_ = 0.0;

};
