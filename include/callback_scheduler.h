#pragma once

#include <functional>
#include <chrono>
#include <cstdint>
#include <memory>

class CallbackScheduler {
public:
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
    using TaskId = std::uint64_t;

    CallbackScheduler();
    ~CallbackScheduler();

    TaskId Schedule(std::function<void()> callback, TimePoint when);
    bool Cancel(TaskId id);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};
