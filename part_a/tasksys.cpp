#include "tasksys.h"
#include <cmath>
#include <cstddef>

#include "CycleTimer.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), 
    max_num_threads_(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


void TaskSystemParallelSpawn::parallelSpawnWork(IRunnable* runnable, int threadId, int num_total_tasks){
    for (int i = threadId; i < num_total_tasks; i += max_num_threads_){
        // printf("[%d], task: %d\n", threadId, i);
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // printf("== TaskSystemParallelSpawn =  num_total_tasks: %d\n", num_total_tasks);

    std::vector<std::thread> work;

    for (int i = 0; i < max_num_threads_; i++){
        work.emplace_back(&TaskSystemParallelSpawn::parallelSpawnWork, this, runnable, i, num_total_tasks);
    }

    for (std::thread &t: work){
        t.join();
    }
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


void TaskSystemParallelThreadPoolSpinning::LaunchSpinningThread(int threadId, 
                            std::shared_ptr<std::atomic<int>> curr_task_id, 
                            std::shared_ptr<std::atomic<int>> task_done){                                
    // printf("started thread: %d\n", threadId);
    int next_task = 0;
    while(!done){
        // we have to make sure that the sync function is release when
        // all the thread are waiting
        if(curr_task_id->load() >= curr_num_total_tasks)
            continue;
        
        // exit the spin and do wore
        while(!done){
            next_task = curr_task_id->fetch_add(1);
            if (next_task >= curr_num_total_tasks)
                break;

            // do one work item and decrement run_done
            curr_runnable->runTask(next_task, curr_num_total_tasks); 
            // mark one done
            task_done->fetch_add(1);
        }  
    }
    // printf("Thread exited\n");
}



TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): 
    ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    printf("TaskSystemParallelThreadPoolSpinning(): num_threads:%d\n", num_threads);

    // done.store(false);
    // run_done.store(0);

    curr_task_id = std::shared_ptr<std::atomic<int>>(new std::atomic<int>(0));
    task_done = std::shared_ptr<std::atomic<int>>(new std::atomic<int>(0));
    curr_num_total_tasks = 0;
    curr_runnable = nullptr;

    for (int i=0; i < num_threads; i++)
        threads.emplace_back(&TaskSystemParallelThreadPoolSpinning::LaunchSpinningThread, 
                this, i, std::shared_ptr<std::atomic<int>>(curr_task_id), std::shared_ptr<std::atomic<int>>(task_done));
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    done = true;
    for (auto &t: threads)
        t.join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    // printf("TaskSystemParallelThreadPoolSpinning::run() - total_tasks: %d\n", num_total_tasks);

    curr_runnable = runnable;
    task_done->store(0);
    curr_num_total_tasks = num_total_tasks;
    // Activate all threasd such that they see their current task id being less 
    // than the total number of tasks 
    curr_task_id->store(0);

    // need to make sure all threads have finished all task
    // use the `done` counter for that
    while(task_done->load() < num_total_tasks){
        continue;
    }    
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


void TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread(int threadId, 
                            std::shared_ptr<std::atomic<int>> curr_task_id, 
                            std::shared_ptr<std::atomic<int>> task_done){                                
    // printf("started thread: %d\n", threadId);
    int next_task = 0;
    while(!done){
        // we have to make sure that the sync function is release when
        // all the thread are waiting
        // if(curr_task_id->load() >= curr_num_total_tasks)
        //     continue;
        
        std::unique_lock<std::mutex> lck(mtx);

        condVar.wait(lck, [&]{ return (done || curr_task_id->load() < curr_num_total_tasks);} );
        lck.unlock();

        // exit the spin and do work
        while(!done){
            next_task = curr_task_id->fetch_add(1);
            if (next_task >= curr_num_total_tasks)
                break;

            // do one work item and decrement run_done
            curr_runnable->runTask(next_task, curr_num_total_tasks); 
            // mark one done
            task_done->fetch_add(1);
        } 
    }
}


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    printf("TaskSystemParallelThreadPoolSleeping(): num_threads:%d\n", num_threads);

    // done.store(false);
    // run_done.store(0);

    curr_task_id = std::shared_ptr<std::atomic<int>>(new std::atomic<int>(0));
    task_done = std::shared_ptr<std::atomic<int>>(new std::atomic<int>(0));
    curr_num_total_tasks = 0;
    curr_runnable = nullptr;

    for (int i=0; i < num_threads; i++)
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread, 
                this, i, std::shared_ptr<std::atomic<int>>(curr_task_id), std::shared_ptr<std::atomic<int>>(task_done));

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    done = true;
    condVar.notify_all();
    for (auto &t: threads)
        t.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // printf("TaskSystemParallelThreadPoolSleeping::run()\n");

    curr_runnable = runnable;
    task_done->store(0);
    curr_num_total_tasks = num_total_tasks;
    // Activate all threasd such that they see their current task id being less 
    // than the total number of tasks 
    curr_task_id->store(0);
    {
        std::lock_guard<std::mutex> lck(mtx);
        condVar.notify_all();
    }

    // need to make sure all threads have finished all task
    // use the `done` counter for that
    while(task_done->load() < num_total_tasks){
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
