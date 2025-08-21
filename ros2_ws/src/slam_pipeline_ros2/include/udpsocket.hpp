#pragma once
#include <cstdint>
#include <vector>
#include <functional>
#include <memory>
#include <string>
#include <optional>
#include <chrono>
#include <stdexcept>
#include <iostream> // Added for std::cerr
#include <boost/asio.hpp>
#include <boost/core/span.hpp>

namespace udp_socket {

    /**
     * @brief Configuration struct for UdpSocket.
     * 
     * Defines parameters for initializing the UDP socket, including host, port, buffer size,
     * timeouts, and socket options like broadcast and multicast.
     */
    struct UdpSocketConfig {
        std::string host;                               ///< Hostname or IP address to bind to
        uint16_t port;                                  ///< Port to bind to
        uint32_t bufferSize = 24832;                    ///< Size of the receive buffer
        std::chrono::milliseconds resolveTimeout{5000}; ///< Timeout for DNS resolution
        bool enableBroadcast = false;                    ///< Enable broadcast option
        std::optional<boost::asio::ip::address> multicastGroup; ///< Multicast group to join (if set)
        std::optional<std::chrono::milliseconds> receiveTimeout; ///< Timeout for packet reception
        std::optional<int> ttl;                         ///< Time-to-live for multicast/broadcast
        bool reuseAddress = false;                      ///< Enable SO_REUSEADDR option
    };

    /**
     * @brief Type aliases for clarity and API consistency.
     */
    using SpanType = boost::span<const uint8_t>;
    using UdpSocketPtr = std::shared_ptr<class UdpSocket>;
    using DataCallback = std::function<void(SpanType)>;
    using ErrorCallback = std::function<void(const boost::system::error_code&)>;

    /**
     * @brief A robust, asynchronous UDP socket class for receiving data.
     * 
     * Designed for high-performance, reliable UDP communication, suitable for applications
     * like autonomous marine vehicles. Supports asynchronous DNS resolution, timeouts,
     * broadcast, multicast, and zero-copy data handling via boost::span.
     * 
     * @note This is a header-only class. Use UdpSocket::create to instantiate.
     * @note The SpanType (boost::span) passed to DataCallback is valid only during the
     *       callback's execution. Use UdpSocket::copySpan to store data beyond the
     *       callback's scope.
     * @note The io_context must outlive the UdpSocket instance.
     * @note Requires C++17 and Boost 1.70+ (for boost::span and Boost.Asio).
     */
    class UdpSocket : public std::enable_shared_from_this<UdpSocket> {
    public:
    
        UdpSocket(
            boost::asio::io_context& context,
            DataCallback dataCallback,
            ErrorCallback errorCallback,
            const UdpSocketConfig& config)
            : socket_(context), resolver_(context), timeoutTimer_(context),
              buffer_(config.bufferSize), dataCallback_(std::move(dataCallback)),
              errorCallback_(std::move(errorCallback)), config_(config) {
            if (config.bufferSize == 0) {
                throw std::invalid_argument("Buffer size must be non-zero");
            }
        }

        /**
         * @brief Factory function to create and initialize a UdpSocket.
         * 
         * @param context The Boost.Asio io_context for asynchronous operations.
         * @param config Configuration parameters for the socket.
         * @param dataCallback Callback invoked with received data.
         * @param errorCallback Callback invoked on errors.
         * @return UdpSocketPtr A shared_ptr to the created UdpSocket.
         * @throws std::invalid_argument If the config is invalid.
         */
        static inline UdpSocketPtr create(
            boost::asio::io_context& context,
            const UdpSocketConfig& config,
            DataCallback dataCallback,
            ErrorCallback errorCallback) {
            if (config.host.empty()) {
                throw std::invalid_argument("Host cannot be empty");
            }
            if (config.port == 0) {
                throw std::invalid_argument("Port must be non-zero");
            }
            if (config.bufferSize == 0) {
                throw std::invalid_argument("Buffer size must be non-zero");
            }
            if (config.multicastGroup && !config.multicastGroup->is_multicast()) {
                throw std::invalid_argument("Invalid multicast group address");
            }
            auto socket = std::make_shared<UdpSocket>(context, std::move(dataCallback), std::move(errorCallback), config);
            socket->start(config.host, config.port);
            return socket;
        }

        /**
         * @brief Destructor. Stops all operations and closes the socket.
         */
        inline ~UdpSocket() { stop(); }

        // Copy and move operations are disabled to ensure safe lifetime management
        // of asynchronous operations. Use UdpSocketPtr to manage the object's lifetime.
        UdpSocket(const UdpSocket&) = delete;
        UdpSocket& operator=(const UdpSocket&) = delete;
        UdpSocket(UdpSocket&&) = delete;
        UdpSocket& operator=(UdpSocket&&) = delete;

        /**
         * @brief Stops all operations and closes the socket.
         */
        inline void stop() {
            boost::system::error_code ec;
            timeoutTimer_.cancel(ec);
            if (ec) {
                std::cerr << "Failed to cancel timer: " << ec.message() << std::endl;
            }
            socket_.cancel(ec);
            if (ec) {
                std::cerr << "Failed to cancel socket: " << ec.message() << std::endl;
            }
            socket_.close(ec);
            if (ec) {
                std::cerr << "Failed to close socket: " << ec.message() << std::endl;
            }
        }

        /**
         * @brief Helper to copy span data if needed beyond callback lifetime.
         * 
         * @param data The span containing received data.
         * @return std::vector<uint8_t> A copy of the data.
         */
        static inline std::vector<uint8_t> copySpan(SpanType data) {
            return {data.begin(), data.end()};
        }

    private:

        inline void start(const std::string& host, uint16_t port) {
            resolver_.async_resolve(
                boost::asio::ip::udp::v4(), host, std::to_string(port),
                [self = shared_from_this()](const boost::system::error_code& ec, auto endpoints) {
                    self->handleResolve(ec, endpoints);
                });
            timeoutTimer_.expires_after(config_.resolveTimeout);
            timeoutTimer_.async_wait([self = shared_from_this()](const boost::system::error_code& ec) {
                if (!ec) {
                    std::cerr << "DNS resolution timed out after " << self->config_.resolveTimeout.count() << " ms" << std::endl;
                    self->resolver_.cancel();
                    if (self->errorCallback_) self->errorCallback_(boost::asio::error::timed_out);
                }
            });
        }

        inline void handleResolve(const boost::system::error_code& ec, const boost::asio::ip::udp::resolver::results_type& endpoints) {
            boost::system::error_code cancelEc;
            timeoutTimer_.cancel(cancelEc);
            if (cancelEc) {
                std::cerr << "Failed to cancel timeout timer: " << cancelEc.message() << std::endl;
            }
            if (ec == boost::asio::error::operation_aborted) {
                return; // Timer fired first, error already reported
            }
            if (ec) {
                if (errorCallback_) errorCallback_(ec);
                std::cerr << "Resolve error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                return;
            }
            setupSocket(endpoints);
        }

        inline void setupSocket(const boost::asio::ip::udp::resolver::results_type& endpoints) {
            boost::system::error_code ec;
            socket_.open(boost::asio::ip::udp::v4(), ec);
            if (ec) {
                if (errorCallback_) errorCallback_(ec);
                std::cerr << "Open error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                return;
            }
            if (config_.reuseAddress) {
                socket_.set_option(boost::asio::socket_base::reuse_address(true), ec);
                if (ec) {
                    if (errorCallback_) errorCallback_(ec);
                    std::cerr << "Set reuse address error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                    return;
                }
            }
            socket_.bind(*endpoints.begin(), ec);
            if (ec) {
                if (errorCallback_) errorCallback_(ec);
                std::cerr << "Bind error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                return;
            }
            if (config_.enableBroadcast) {
                socket_.set_option(boost::asio::socket_base::broadcast(true), ec);
                if (ec) {
                    if (errorCallback_) errorCallback_(ec);
                    std::cerr << "Set broadcast error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                    return;
                }
            }
            if (config_.multicastGroup) {
                socket_.set_option(boost::asio::ip::multicast::join_group(*config_.multicastGroup), ec);
                if (ec) {
                    if (errorCallback_) errorCallback_(ec);
                    std::cerr << "Join multicast group error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                    return;
                }
            }
            if (config_.ttl) {
                socket_.set_option(boost::asio::ip::multicast::hops(*config_.ttl), ec);
                if (ec) {
                    if (errorCallback_) errorCallback_(ec);
                    std::cerr << "Set TTL error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
                    return;
                }
            }
            startReceive();
        }

        inline void startReceive() {
            socket_.async_receive_from(
                boost::asio::buffer(buffer_), senderEndpoint_,
                [self = shared_from_this()](const boost::system::error_code& ec, std::size_t bytesReceived) {
                    self->handleReceive(ec, bytesReceived);
                });
            if (config_.receiveTimeout) {
                timeoutTimer_.expires_after(*config_.receiveTimeout);
                timeoutTimer_.async_wait([self = shared_from_this()](const boost::system::error_code& ec) {
                    if (!ec) {
                        std::cerr << "Receive timed out after " << self->config_.receiveTimeout->count() << " ms" << std::endl;
                        self->socket_.cancel();
                        if (self->errorCallback_) self->errorCallback_(boost::asio::error::timed_out);
                    }
                });
            }
        }

        inline void handleReceive(const boost::system::error_code& ec, std::size_t bytesReceived) {
            assert(bytesReceived <= buffer_.size() && "Buffer overrun detected");
            if (config_.receiveTimeout) {
                boost::system::error_code cancelEc;
                timeoutTimer_.cancel(cancelEc);
                if (cancelEc) {
                    std::cerr << "Failed to cancel receive timeout timer: " << cancelEc.message() << std::endl;
                }
            }
            if (!ec && bytesReceived > 0) {
                // Note: The SpanType is valid only during the callback's execution.
                // Use UdpSocket::copySpan to store the data beyond this scope.
                dataCallback_(SpanType(buffer_.data(), bytesReceived));
            } else if (ec == boost::asio::error::operation_aborted) {
                // No action needed
            } else if (ec) {
                if (errorCallback_) errorCallback_(ec);
                std::cerr << "Receive error: " << ec.message() << " (value: " << ec.value() << ")" << std::endl;
            } else {
                if (errorCallback_) errorCallback_(boost::asio::error::no_data);
                std::cerr << "No data received (bytes: " << bytesReceived << ")" << std::endl;
            }
            if (socket_.is_open()) {
                startReceive();
            }
        }

        boost::asio::ip::udp::socket socket_;
        boost::asio::ip::udp::resolver resolver_;
        boost::asio::ip::udp::endpoint senderEndpoint_;
        boost::asio::steady_timer timeoutTimer_;
        std::vector<uint8_t> buffer_;
        DataCallback dataCallback_;
        ErrorCallback errorCallback_;
        UdpSocketConfig config_;
    };

} // namespace udp_socket