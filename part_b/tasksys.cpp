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
    num_threads_(get_max_thread_pool_size(num_threads)), task_queue_mutexes(num_threads_), condVarThreads(num_threads_) {

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
    // printf("TaskSystemParallelThreadPoolSleeping destroy!\n");
    done = true;
    for (int i =0; i< num_threads_; i++){
        condVarThreads[i].notify_one();
    }    
    
    for (auto &t: threads)
        t.join();
}


bool TaskSystemParallelThreadPoolSleeping::handle_task_done(TaskID task_done){
    // Update state for completed task and look at the dependency structure to
    //  figure out which tasks need to be scheduled
    
    // remove the task from the queue

    // Check whether dependent tasks need to be waken up
    if (dep_to_tasks.size() == 0)
        return false;

    // printf("Tasks: %d is done, check waiting list\n", taskIdDone);

    auto dep_task_iter = dep_to_tasks.find(task_done);
    if (dep_task_iter == dep_to_tasks.end()){
        // no tasks are depending on the completed one, continue
        return false;
    }
    
    int new_tasks_scheduled = 0;
    auto dependencies = dep_task_iter->second;
    for (auto it = dependencies.begin() ; it != dependencies.end(); ++it){
        // Update the dep list for all tasks that depend on the one just completeed
        auto task = (*it);
        task->deps().erase(task_done);
        // if a task has no more dependencies left, schedule it
        if (task->deps().size() == 0){
            // printf("Tasks: %d has no more dependencies, schedule it!\n", task->id());
            waiting_tasks.erase(task->id());
            task->set_state(TaskState::RUNNING);
            running_tasks[task->id()] = task;
            for (int i = 0; i < task->num_total_tasks_; i++){
                int qid = i % num_threads_;
                std::lock_guard<std::mutex> lck( task_queue_mutexes[qid].m);
                task_queue[qid].push_back(task);
            }
            new_tasks_scheduled++;
        }
    }

    // 
    dep_to_tasks.erase(task_done);

    // if things changes, notify other threads in the thread pool
    
    // printf("New tasks scheduled: %d\n", new_tasks_scheduled);
    return new_tasks_scheduled > 0;
}

void TaskSystemParallelThreadPoolSleeping::LaunchSleepingThread(int threadId){                                
    // printf("started thread: %d\n", threadId);
    // Each thread operates on its own queues and signal the main thread when a task is done
    std::deque<TaskRef> *q = &task_queue[threadId];

    while(!done){        
        // Sleep while waiting for new tasks to arrive, or for Task System shutfown      
        std::unique_lock<std::mutex> lck(threads_mtx);
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

        // pick the next available task to run
        while (!done) 
        {             
            // Pick the next task to work on from the active running queue.
            
            std::unique_lock<std::mutex> lck(task_queue_mutexes[threadId].m);
            // printf("[thread-%d] - Queue size: %ld\n", threadId, q->size());
            if (q->size()==0)
                break;
            TaskRef current_task_ref = q->front();
            q->pop_front();
            lck.unlock();
            // printf("[thread-%d] - task %d, current_task_ref: %p\n", threadId, current_task_ref->id(), current_task_ref.get());

            current_task_ref->do_work(threadId);

            // printf("[thread-%d] - Working on task: %d\n", threadId, current_task_ref->id());
            // printf("[thread-%d]== loop() - done Working on task: %d - state: %d, is completed %d\n", 
            //         threadId, 
            //         next_task->id(),
            //         next_task->in_done_state(),
            //         next_task->is_task_completed());

            // notify main thread that a task was completed            
            if (current_task_ref->is_task_completed() && !current_task_ref->complete_notification_sent 
                    && !current_task_ref->in_done_state()){ 
                std::unique_lock<std::mutex> lck(sync_mtx);
                // printf("[thread-%d] - task %d completed, send notification\n", threadId, current_task_ref->id());
                current_task_ref->complete_notification_sent = true;
                current_task_ref->set_state(TaskState::DONE);
                task_completed_queue.emplace_back(current_task_ref->id());
                lck.unlock();
                sync_condVar.notify_one();                
                // double end = CycleTimer::currentSeconds();
                // printf("[thread-%d]== loop() - task: %d completed and schedule waiting tasks took %.03fms\n", threadId, current_task.id, (end - start)*1000);                
            }
        }           
    }

    // printf("[Thread-%d] Exited\n", threadId);
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    // There can be onnly 1 task in the running queue
    TaskID curr_tid = last_task_id;
    last_task_id++;

    // printf("TaskSystemParallelThreadPoolSleeping::run - Task: %d\n", curr_tid);

    TaskRef task = Task::create(curr_tid, runnable, num_total_tasks, {});
    task->set_state(TaskState::RUNNING);

    // add task to queues

    running_tasks[curr_tid] = task;
    for (int i = 0; i < num_total_tasks; i++){
        int qid = i % num_threads_;
        {
            std::lock_guard<std::mutex> lck(task_queue_mutexes[qid].m);
            task_queue[qid].emplace_back(task);  
            condVarThreads[qid].notify_one();
        }

        // printf("Add task %d to queue %d\n", curr_tid, qid);
    }
   
    sync();
}


// need to map a dependency to the task

bool TaskSystemParallelThreadPoolSleeping::process_dependencies(TaskRef task, std::vector<TaskID>& deps){
    bool can_run = true;
    std::vector<TaskID> final_deps;

    // Check whether this tasks needs to wait for its dependent task to finish. If the dependent task
    // are still running, add this task to the waiting list
    for (auto tid: deps){
        bool running = false;
        bool waiting = false;

        // Check if dependent task is still running
        auto task_it = running_tasks.find(tid);    
        if (task_it != running_tasks.end()){
            // dependent task is currently running
            can_run = false;
            running = true;
        }

        // Check if dependent task is in the waiting list
        auto waiting_it = waiting_tasks.find(tid);
        if (waiting_it != waiting_tasks.end()){
            waiting = true;
            can_run = false;
        }
        
        if (waiting || running){
            // dependent tasks haven't yet finished, this task wait on this dependency
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

    /** 
     * Check if dep listed in waiting list:
     *   - if all deps are, create a new task add it add it to the waiting list
     *   - if only some dep are, add the task and the dep that are not in the waiting list (dep first)
     *   - if no deps are in the waiting list, check if any are running. 
     *         - if all are running, just add the task in the waiting list
     *         - if some are running, add the deps that are not running in the waiting task, together with the task
     *         - if no dep are running, and nothing is running, put the dep in the waiting 
     *  
     * */                                                    

    // printf("\n\n~~ runAsyncWithDeps()\n");
    // double start = CycleTimer::currentSeconds();

    // check deps
    TaskID curr_tid = last_task_id;
    last_task_id++;
    
    {
        std::vector<TaskID> task_deps(deps);
        // create the task
        // double start = CycleTimer::currentSeconds();

        TaskRef task = Task::create(curr_tid, runnable, num_total_tasks, task_deps);

        bool can_run = process_dependencies(task, task_deps);
        // printf("Task %d can run: %d", curr_tid, can_run);

        if (!can_run){            
            task->set_state(TaskState::WAITING);
            waiting_tasks.insert(task->id());
        } else {
            task->set_state(TaskState::RUNNING);
            running_tasks[curr_tid] = task;
            for (int i = 0; i < num_total_tasks; i++){
                int qid = i % num_threads_;
                {
                    std::lock_guard<std::mutex> lck(task_queue_mutexes[qid].m);
                    task_queue[qid].emplace_back(task);   
                }
                condVarThreads[qid].notify_one();

                // printf("Add task %d to queue %d", curr_tid, qid);
            }
        }

        // double end = CycleTimer::currentSeconds();
        // printf("Task %d Created and processed! took: %.03fms \n", curr_tid, (end - start)*1000);

        // print_tasks(tasks);
        // print_waiting(waiting_tasks);

    }

    // double start_notify = CycleTimer::currentSeconds();
    
    // double end_notify = CycleTimer::currentSeconds();
    // printf("Task %d notify all threads! took: %.03fms \n", curr_tid, (end_notify - start_notify)*1000);

    // double end = CycleTimer::currentSeconds();
    // printf("Task %d Created done! took: %.03fms \n", curr_tid, (end - start)*1000);

    return curr_tid;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    // printf("Sync start\n");
    // check the work progress
    // double start = CycleTimer::currentSeconds();

    // std::unique_lock<std::mutex> lck(sync_mtx);
    std::unique_lock<std::mutex> sync_lck(sync_mtx, std::defer_lock);

    while(!done){        

        // Wait on work to be completed by the worker threads
        sync_lck.lock();
        sync_condVar.wait(sync_lck, [&]{ 
                return done || task_completed_queue.size() != 0; });

        bool notify_threads = false;        
        TaskID tid_completed = task_completed_queue.front();
        task_completed_queue.pop_front();

        sync_lck.unlock();


        // printf("Sync - Got task %d completed\n", tid_completed);

        // Clear state for completed tasks
        auto it = running_tasks.find(tid_completed);
        if (it != running_tasks.end()){
            // (*it).second->set_state(TaskState::DONE);
            notify_threads = handle_task_done(tid_completed);
            running_tasks.erase(it);
        }
        
        if (notify_threads){
            for (int i = 0; i < num_threads_; i++)
                condVarThreads[i].notify_one();
        }
        
        // printf("Sync thread - Waiting: %ld running: %ld\n", waiting_tasks.size(), running_tasks.size());
        // We are done when there are no more running tasks and waiting tasks
        if (waiting_tasks.size() == 0 && running_tasks.size() == 0){
            // printf("All done!\n");
            break;    
        }
    }

    running_tasks.clear();
    waiting_tasks.clear();
    dep_to_tasks.clear();
    // double end = CycleTimer::currentSeconds();

    // printf("Sync done! took: %.03fms \n", (end - start)*1000);
}
