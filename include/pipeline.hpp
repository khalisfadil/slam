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

#include <udpsocket.hpp>

#include <LidarDataframe.hpp>
#include <LidarIMUDataFrame.hpp>

#include <OusterLidarCallback.hpp>
#include <odometry/steam_lo.hpp>

#include <callback_gnssComp.hpp>
#include <DataFrame_ID20.hpp>
#include <DataFrame_ID25.hpp>
#include <DataFrame_ID26.hpp>
#include <DataFrame_ID28.hpp>
#include <DataFrame_ID29.hpp>

#include <navMath.hpp>

class SLAMPipeline {

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

        explicit SLAMPipeline(const std::string& slam_registration, const std::string& odom_json_path, const std::string& lidar_json_path, const lidarDecode::OusterLidarCallback::LidarTransformPreset& T_preset);
        static void signalHandler(int signal);
        void setThreadAffinity(const std::vector<int>& coreIDs);

        //### application listener
        void runOusterLidarListenerSingleReturn(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores);
        void runOusterLidarListenerLegacy(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores); 
        void runGNSSID20Listener(boost::asio::io_context& ioContext, udp_socket::UdpSocketConfig udp_config, const std::vector<int>& allowedCores); 

        // application for logging
        void processLogQueue(const std::string& filename, const std::vector<int>& allowedCores);
        void logMessage(const std::string& level, const std::string& message);

        // application for slam
        void dataAlignmentID20(const std::vector<int>& allowedCores);
        void runLoStateEstimation(const std::vector<int>& allowedCores);
        void runGroundTruthEstimation(const std::string& filename, const std::vector<int>& allowedCores);

        void saveOdometryResults(const std::string& timestamp);

    private:

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
        uint16_t frame_id_= 0;

        // runGNSSListener
        decodeNav::GnssCompassCallback gnssCallback_;
        std::deque<decodeNav::DataFrameID20> gnss_data_window_;
        double unixTime = 0.0;
        const size_t DATA_SIZE_GNSS = 15;

        // runOusterLidarIMUListener
        lidarDecode::LidarIMUDataFrame temp_IMU_data_;
        std::vector<lidarDecode::LidarIMUDataFrame> temp_IMU_vec_data_;
        const size_t VECTOR_SIZE_IMU = 15;

        // runOusterLidarIMUListener
        uint64_t Normalized_Timestamp_s_ = 0.0;

        unsigned int num_threads_ = 4;

}; // namespace SLAMPipeline