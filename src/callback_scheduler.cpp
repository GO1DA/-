#include "callback_scheduler.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <atomic>
#include <unordered_map>
#include <optional>

class CallbackScheduler::Impl {
public:
    struct Task {
        TaskId id;
        TimePoint when;
        std::function<void()> callback;
        
        bool operator>(const Task& other) const {
            return when > other.when;
        }
    };

    Impl() : nextId(1), stopFlag(false) {
        worker = std::thread(&Impl::WorkerLoop, this);
    }

    ~Impl() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopFlag = true;
            cv.notify_all();
        }
        if (worker.joinable()) {
            worker.join();
        }
    }

    TaskId Schedule(std::function<void()> callback, TimePoint when) {
        TaskId id = nextId++;
        
        {
            std::lock_guard<std::mutex> lock(mutex);
            // Проверяем, не был ли уже отменён этот ID (но мы только что его создали, так что нет)
            Task task{id, when, std::move(callback)};
            tasks.push(std::move(task));
            pendingTasks[id] = true; // задача активна
            cv.notify_one();
        }
        
        return id;
    }

    bool Cancel(TaskId id) {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = pendingTasks.find(id);
        if (it != pendingTasks.end() && it->second) {
            it->second = false; // помечаем как отменённую
            return true;
        }
        return false;
    }

private:
    void WorkerLoop() {
        while (true) {
            std::unique_lock<std::mutex> lock(mutex);
            
            cv.wait(lock, [this] {
                return stopFlag || !tasks.empty();
            });
            
            if (stopFlag && tasks.empty()) {
                break;
            }
            
            if (!tasks.empty()) {
                auto now = std::chrono::system_clock::now();
                auto topTask = tasks.top();
                
                if (topTask.when <= now) {
                    // Задача готова к выполнению
                    tasks.pop();
                    
                    // Проверяем, не отменена ли задача
                    auto it = pendingTasks.find(topTask.id);
                    if (it != pendingTasks.end() && it->second) {
                        // Удаляем из pendingTasks перед выполнением
                        pendingTasks.erase(it);
                        lock.unlock();
                        
                        // Выполняем колбек с защитой от исключений
                        try {
                            if (topTask.callback) {
                                topTask.callback();
                            }
                        } catch (...) {
                            // Исключения не выходят за пределы рабочего потока
                        }
                        
                        lock.lock();
                    } else {
                        // Задача была отменена, просто удаляем её
                        pendingTasks.erase(topTask.id);
                    }
                    continue; // продолжаем цикл, не ждём
                } else {
                    // Ждём до времени следующей задачи
                    cv.wait_until(lock, topTask.when);
                }
            }
        }
    }

    std::priority_queue<Task, std::vector<Task>, std::greater<Task>> tasks;
    std::unordered_map<TaskId, bool> pendingTasks;
    std::mutex mutex;
    std::condition_variable cv;
    std::thread worker;
    std::atomic<TaskId> nextId;
    bool stopFlag;
};

CallbackScheduler::CallbackScheduler() : pImpl(std::make_unique<Impl>()) {}
CallbackScheduler::~CallbackScheduler() = default;

CallbackScheduler::TaskId CallbackScheduler::Schedule(std::function<void()> callback, TimePoint when) {
    return pImpl->Schedule(std::move(callback), when);
}

bool CallbackScheduler::Cancel(TaskId id) {
    return pImpl->Cancel(id);
}
