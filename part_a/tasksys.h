#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <queue>
#include <mutex>
#include <memory>
#include <thread>
#include <condition_variable>
#include <atomic>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        void parallelSpawnWork(IRunnable* runnable, int threadId, int num_total_tasks);

        // the max number of threads this System can use
        int max_num_threads_;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    
    private:
        void LaunchSpinningThread(int threadId);

        // When destroying the thread pool, use these flag to ensure threads exit their
        // loop and cleanup
        bool done; 

        // Keep track of all threads launched by this thread pool
        std::vector<std::thread> threads;

        // Use the `curr_task_id` as an implicit ticket queue from which thread
        // pickup the next task id to work on
        std::shared_ptr<std::atomic<int>> curr_task_id;
        
        // Task increment this counter when they are done with 1 piece of work
        std::shared_ptr<std::atomic<int>> task_done;

        // Each call to run() will need to set the runnable and total number of tasks
        IRunnable *curr_runnable;
        int  curr_num_total_tasks;

};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        void LaunchSleepingThread(int threadId);

        // When destroying the thread pool, use these flag to ensure threads exit their
        // loop and cleanup
        bool done; 

        // Keep track of all threads launched by this thread pool
        std::vector<std::thread> threads;

        // Use the `curr_task_id` as an implicit ticket queue from which thread
        // pickup the next task id to work on
        std::shared_ptr<std::atomic<int>> curr_task_id;
        
        // Task increment this counter when they are done with 1 piece of work
        std::shared_ptr<std::atomic<int>> task_done;

        // 
        std::condition_variable condVar;
        std::mutex mtx;

        // Each call to run() will need to set the runnable and total number of tasks
        IRunnable *curr_runnable;
        int  curr_num_total_tasks;
};

#endif
