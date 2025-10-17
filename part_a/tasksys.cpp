#include "tasksys.h"
#include <cmath>
#include <cstddef>
#include <chrono>
#include <sched.h>

#include "CycleTimer.h"
IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

int MAX_HW_THREADS = std::thread::hardware_concurrency();
int get_max_thread_pool_size(int nthreads){
    // printf("MAX_HW_THREADS: %d\n", MAX_HW_THREADS);
    return std::min(MAX_HW_THREADS - 1, nthreads);
}

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(get_max_thread_pool_size(num_threads)), 
    max_num_threads_(get_max_thread_pool_size(num_threads)) {
    
    curr_task_id = std::shared_ptr<std::atomic<int>>(new std::atomic<int>(0));

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


void TaskSystemParallelSpawn::parallelSpawnWork(IRunnable* runnable, int threadId, int num_total_tasks){
    int next_task = 0;


    while (true) {
        (next_task = curr_task_id->fetch_add(1));   
        if (next_task >= num_total_tasks)
            break;
        runnable->runTask(next_task, num_total_tasks);
    }

}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<std::thread> work;

    // printf("TaskSystemParallelSpawn::run()\n");
    // double start = CycleTimer::currentSeconds();

    curr_task_id->store(0);
    for (int i = 0; i < max_num_threads_; i++){
        work.emplace_back(&TaskSystemParallelSpawn::parallelSpawnWork, this, runnable, i, num_total_tasks);
    }

    for (std::thread &t: work){
        t.join();
    }

    // double end = CycleTimer::currentSeconds();
    // printf("total time: %0.03fms\n", (end - start)* 1000);

}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}


void TaskSystemParallelThreadPoolSpinning::LaunchSpinningThread(int threadId){                                
    int next_task = 0;

    while(!done){
        // Wait for work to arrive
        if(curr_task_id.load() >= curr_num_total_tasks)
            continue;

        // printf("[run-%d][thread-%d] start work\n", bulk_run_id, threadId);
        // loop until no more work left to do
        int work_done = 0;
        while (true) {
            next_task = curr_task_id.fetch_add(1);

            if (next_task >= curr_num_total_tasks)
                break;            
            // do one work item and decrement run_done
            curr_runnable->runTask(next_task, curr_num_total_tasks); 
            // mark one task done
            work_done++;
        }  

        tasks_completed.fetch_add(work_done);
    }
}



TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): 
    ITaskSystem(get_max_thread_pool_size(num_threads)), max_num_threads_(get_max_thread_pool_size(num_threads)) {

    curr_task_id.store(max_num_threads_);
    tasks_completed.store(0);
    curr_num_total_tasks = 0;
    curr_runnable = nullptr;

    for (int i=0; i < max_num_threads_; i++)
        threads.emplace_back(&TaskSystemParallelThreadPoolSpinning::LaunchSpinningThread, this, i);
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    done = true;
    for (auto &t: threads)
        t.join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    bulk_run_id++;
    curr_runnable = runnable;
    tasks_completed.store(0);
    curr_num_total_tasks = num_total_tasks;
    // Activate all threasd such that they see their current task id being less 
    // than the total number of tasks 
    curr_task_id.store(0);

    // need to make sure all threads have finished all task
    // use the `done` counter for that
    while(tasks_completed.load() < num_total_tasks)
        continue;

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}


void TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread(int threadId){                                
    // printf("started thread: %d\n", threadId);
    int next_task = 0;
    while(!done){
        
        // Sleep while waiting for new work to arrive, or for Task System shutfown
        std::unique_lock<std::mutex> lck(threads_mtx);
        condVarThreads.wait(lck, [&]{ return (done || curr_task_id.load() < curr_num_total_tasks);} );
        lck.unlock();

        // Now get to work!
        int work_done = 0;
        while(true){
            next_task = curr_task_id.fetch_add(1);
            if (next_task >= curr_num_total_tasks)
                break;
            // do one work item and decrement run_done
            curr_runnable->runTask(next_task, curr_num_total_tasks); 
            work_done++;
        } 
        tasks_completed.fetch_add(work_done);
    }
}


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(get_max_thread_pool_size(num_threads)) {

    curr_task_id.store(0);
    tasks_completed.store(0);
    curr_num_total_tasks = 0;
    curr_runnable = nullptr;

    for (int i=0; i < get_max_thread_pool_size(num_threads); i++)
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread, this, i);

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Notify all threads to exit their loops
    done = true;
    {
        std::lock_guard<std::mutex> lck(threads_mtx);
        condVarThreads.notify_all();
    }    
        
    for (auto &t: threads)
        t.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    curr_runnable = runnable;
    tasks_completed.store(0);
    curr_num_total_tasks = num_total_tasks;
    // Activate all threasd such that they see their current task id being less 
    // than the total number of tasks 
    curr_task_id.store(0);
    {
        std::lock_guard<std::mutex> lck(threads_mtx);
        condVarThreads.notify_all();
    }

    // need to make sure all threads have finished all task
    // use the `done` counter for that
    while(tasks_completed.load() < num_total_tasks){
        continue;
    }

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
