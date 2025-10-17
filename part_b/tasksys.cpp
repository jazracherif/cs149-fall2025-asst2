#include "tasksys.h"
#include "CycleTimer.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

int MAX_HW_THREADS = std::thread::hardware_concurrency();
int get_max_thread_pool_size(int nthreads){
    // printf("MAX_HW_THREADS: %d\n", MAX_HW_THREADS);
    return std::min(MAX_HW_THREADS - 1, nthreads);
}


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(get_max_thread_pool_size(num_threads)), 
    num_threads_(get_max_thread_pool_size(num_threads)), condVarThreads(num_threads_), task_queue_mutexes(num_threads_) {

    // printf("\n\nTaskSystemParallelThreadPoolSleeping()\n\n");

    for (int i=0; i < num_threads_; i++){
        task_queue.emplace_back(std::deque<TaskRef>());

    }
    for (int i=0; i < num_threads_; i++){
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread, this, i);
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    // Notify all threads to exit their loops
    done = true;
    for (int i =0; i< num_threads_; i++){
        condVarThreads[i].notify_one();
    }    
    
    for (auto &t: threads)
        t.join();
}


bool TaskSystemParallelThreadPoolSleeping::handle_task_done(TaskID task_done){
    // Update state for completed task and look at the dependency structure to
    // figure out which tasks need to be scheduled    
    if (dep_to_tasks.size() == 0)
        return false;

    // printf("Tasks: %d is done, check waiting list\n", taskIdDone);

    // Are there any tasks that depend on the just completed one?
    auto dep_task_iter = dep_to_tasks.find(task_done);
    if (dep_task_iter == dep_to_tasks.end()){
        return false;
    }
    
    // yet, update the dep list for each tasks that depend on the one just completeed
    int new_tasks_scheduled = 0;
    auto dependencies = dep_task_iter->second;
    for (auto it = dependencies.begin() ; it != dependencies.end(); ++it){
        auto task = (*it);
        task->deps().erase(task_done);

        // if a task has no more dependencies left, schedule it!
        if (task->deps().size() == 0){
            // printf("Tasks: %d has no more dependencies, schedule it!\n", task->id());
            waiting_tasks.erase(task->id());
            task->set_state(TaskState::RUNNING);
            running_tasks[task->id()] = task;
            // notify all threads of new work available
            for (int i = 0; i < num_threads_; i++){               
                std::lock_guard<std::mutex> lck(task_queue_mutexes[i].m);
                task_queue[i].emplace_back(task);  
            }            
            new_tasks_scheduled++;
        }
    }

    // cleanup
    dep_to_tasks.erase(task_done);
    
    // printf("New tasks scheduled: %d\n", new_tasks_scheduled);
    return new_tasks_scheduled > 0;
}

void TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread(int threadId){                                
    // Each thread operates on its own queues and signal the main thread when a task is done
    std::deque<TaskRef> *q = &task_queue[threadId];

    while(!done){        
        // Sleep while waiting for new tasks to arrive, or for Task System shutfown      
        std::unique_lock<std::mutex> lck(task_queue_mutexes[threadId].m);
        condVarThreads[threadId].wait(lck, [&]{ 
                if (done)
                    return true;
                if (q->size() != 0)
                    return true;
                // printf("[thread-%d] Sleep ..\n", threadId);
                return false;
            });               
        lck.unlock();

        // printf("[thread-%d]== loop() - Up!\n", threadId);

        // main working loop
        while (!done) 
        {             
            // Pick the next task to work on from the threa'ds own queue.
            std::unique_lock<std::mutex> lck(task_queue_mutexes[threadId].m);
            // printf("[thread-%d] - Queue size: %ld\n", threadId, q->size());
            if (q->size()==0)
                break;
            TaskRef current_task_ref = q->front();
            q->pop_front();
            lck.unlock();
            // printf("[thread-%d] - task %d, current_task_ref: %p\n", threadId, current_task_ref->id(), current_task_ref.get());

            current_task_ref->do_work(threadId);

            // notify main thread that a task was completed            
            if (current_task_ref->is_task_completed() && !current_task_ref->complete_notification_sent 
                    && !current_task_ref->in_done_state()){ 
                std::unique_lock<std::mutex> lck(sync_mtx);
                // printf("[thread-%d] - task %d completed, send notification\n", threadId, current_task_ref->id());

                // Update this task state and sent a notification to the main thread
                current_task_ref->complete_notification_sent = true;
                current_task_ref->set_state(TaskState::DONE);
                task_completed_queue.emplace_back(current_task_ref->id());
                lck.unlock();
                sync_condVar.notify_one();                
            }
        }           
    }

    // printf("[Thread-%d] Exited\n", threadId);
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    // There can be onnly 1 task in the running queue
    TaskID curr_tid = next_task_id;
    next_task_id++;

    // printf("TaskSystemParallelThreadPoolSleeping::run - Task: %d\n", curr_tid);

    TaskRef task = Task::create(curr_tid, runnable, num_total_tasks, {});

    // Scheduled the task to run
    task->set_state(TaskState::RUNNING);
    running_tasks[curr_tid] = task;

    // Wake up threads for new work
    for (int i = 0; i < num_threads_; i++){
        {
            std::lock_guard<std::mutex> lck(task_queue_mutexes[i].m);
            task_queue[i].emplace_back(task);
        }
        condVarThreads[i].notify_one();
    }
    // printf("Add task %d to queue %d\n", curr_tid, qid);

    // Now block on sync for the work to finish
    sync();
}


bool TaskSystemParallelThreadPoolSleeping::process_dependencies(TaskRef task, std::vector<TaskID>& deps){
    bool can_run = true;
    std::vector<TaskID> final_deps;

    // Check whether this tasks needs to wait for its dependent tasks to finish. If the dependent tasks
    // are still running, add this task to the waiting list
    for (auto tid: deps){
        bool running = false;
        bool waiting = false;

        // dep still running?
        auto task_it = running_tasks.find(tid);    
        if (task_it != running_tasks.end()){
            running = true;
            can_run = false;
        }

        // dep in the waiting list?
        auto waiting_it = waiting_tasks.find(tid);
        if (waiting_it != waiting_tasks.end()){
            waiting = true;
            can_run = false;
        }
        
        // if the dependent task have not yet completed, keep it in the dep list
        if (waiting || running){
            final_deps.push_back(tid);
            dep_to_tasks[tid].insert(task);
        } else {
            // printf("Dependent task: %d not needed anymore!\n", tid);
        }
    }

    // update dependencies
    deps = final_deps;
    
    return can_run;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {                                                        

    // printf("\n\n~~ runAsyncWithDeps()\n");

    TaskID curr_tid = next_task_id;
    next_task_id++;
    
    // create the task
    std::vector<TaskID> task_deps(deps);
    TaskRef task = Task::create(curr_tid, runnable, num_total_tasks, task_deps);

    // Validate and update dependencies based on current state of tasks
    bool can_run = process_dependencies(task, task_deps);

    // 
    if (!can_run){            
        task->set_state(TaskState::WAITING);
        waiting_tasks.insert(task->id());
    } else {
        task->set_state(TaskState::RUNNING);
        running_tasks[curr_tid] = task;
        for (int i = 0; i < num_threads_; i++){
            {
                std::lock_guard<std::mutex> lck(task_queue_mutexes[i].m);
                task_queue[i].emplace_back(task);   
            }
            condVarThreads[i].notify_one();

            // printf("Add task %d to queue %d", curr_tid, qid);
        }
    }


    return curr_tid;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    // printf("Sync start\n");

    /* define the lock in defer mode and use it in the loop */
    std::unique_lock<std::mutex> sync_lck(sync_mtx, std::defer_lock);

    while(!done){        

        // Wait for notification from threads that work has been completed
        sync_lck.lock();
        sync_condVar.wait(sync_lck, [&]{ 
                return done || task_completed_queue.size() != 0; });

        // pick up the completed task from the task queue
        TaskID tid_completed = task_completed_queue.front();
        task_completed_queue.pop_front();
        sync_lck.unlock();

        // printf("Sync - Got task %d completed\n", tid_completed);

        // reset flag
        bool notify_threads = false;        

        // Clear state for completed tasks and check whether waiting tasks
        // can now be scheduled
        auto it = running_tasks.find(tid_completed);
        if (it != running_tasks.end()){
            notify_threads = handle_task_done(tid_completed);
            running_tasks.erase(it);
        }
        // wake up threads to do work on newly scheduled tasks
        if (notify_threads){
            for (int i = 0; i < num_threads_; i++)
                condVarThreads[i].notify_one();
        }
        
        // printf("Sync thread - Waiting: %ld running: %ld\n", waiting_tasks.size(), running_tasks.size());

        // Check whether all work is done
        if (waiting_tasks.size() == 0 && running_tasks.size() == 0){
            // printf("All done!\n");
            break;    
        }
    }

    running_tasks.clear();
    waiting_tasks.clear();
    dep_to_tasks.clear();

}
