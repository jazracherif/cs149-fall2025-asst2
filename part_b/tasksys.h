#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <deque>
#include <condition_variable>
#include <atomic>
#include <mutex>
#include <thread>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <cstdio>
#include <string>
#include "itasksys.h"

class Task;

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
};

enum class TaskState {
    UNINITIALIZED, /* 0 */
    RUNNING,
    WAITING,
    DONE
};


typedef std::shared_ptr<Task> TaskRef;

class Task {
    public:
        static TaskRef create(TaskID id, IRunnable *runnable, int num_total_tasks, std::vector<TaskID> deps){

            // printf("Create Task %d - num_total_tasks %d -  n_dep %zu: ", id, num_total_tasks, deps.size());
            // for (auto tid_: deps){
            //     printf("%d,", tid_);
            // }
            // printf("\n");
            return std::shared_ptr<Task>(new Task(id, runnable, num_total_tasks, std::unordered_set<TaskID>(deps.begin(), deps.end())));
        }

        bool is_task_completed(){
            // printf("task: %d completed: %d - total: %d\n", id(), tasks_completed_.load(), num_total_tasks_ );
            return tasks_completed_.load() >= num_total_tasks_ ;
        } 

        bool can_do_more(){
            return curr_task_id_.load() < num_total_tasks_;
        }

        bool in_done_state(){
            return state_ == TaskState::DONE;
        } 

        void set_state(TaskState state){
            // printf("Tasks %d - new state: %s\n", id_, to_string(state).c_str());
            state_ = state;
        } 

        TaskState state(){
            return state_;
        } 
        
        TaskID id() { return id_; }

        bool is(TaskID other) { return id_ == other; }

        void do_work(int threadId) { // thread safe
            int next_task = 0;
            int work_done = 0;

            while(true){
                next_task = curr_task_id_.fetch_add(1);
                if (next_task >= num_total_tasks_)
                    break;
                // printf("[thread-%d]== do_work() - task: %d sub tasks: %d - curr_task_id_: %d num_total_tasks: %d  \n", threadId, id_, next_task, curr_task_id_.load(), num_total_tasks_);
                // do one work item and decrement run_done
                runnable_->runTask(next_task, num_total_tasks_); 
                work_done++;
            }

            // update work done
            tasks_completed_.fetch_add(work_done);
        }      

        std::unordered_set<TaskID>& deps() { return deps_; }

        static std::string to_string(TaskState state){
            switch(state){
                case TaskState::UNINITIALIZED: return std::string("UNINITIALIZED");
                case TaskState::RUNNING: return std::string("RUNNING");
                case TaskState::WAITING: return std::string("WAITING");
                case TaskState::DONE: return std::string("DONE");
                default:
                    return std::string("ERROR");
            }
        }

        TaskID id_;     

        // The Runnable task
        IRunnable *runnable_;

        // The total number of tasks to run
        int num_total_tasks_;

    private:
        Task(TaskID id, IRunnable *runnable, int num_total_tasks, std::unordered_set<TaskID> deps): 
            id_(id), runnable_(runnable), num_total_tasks_(num_total_tasks), deps_(deps) {}


        // The state of the task
        TaskState state_;   

    
        // The dependency for this task
        std::unordered_set<TaskID> deps_;

        // keep track of work done on this task
        std::atomic<int> curr_task_id_;
        std::atomic<int> tasks_completed_;
    };


typedef struct taskItem {
    TaskID id;
    IRunnable* runnable;
    int num_total_tasks;    
    int task_id;
} taskItem;

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

        void runOld(IRunnable* runnable, int num_total_tasks);
        void run(IRunnable* runnable, int num_total_tasks);

        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        int num_threads_;

        TaskID last_task_id{0};

         // for modifying the shared task states across threads
        std::mutex q_mtx;

        std::unordered_set<TaskID> waiting_tasks; // in order of call to runAsync
        // std::vector<Task> running_tasks; // threads can pickup any work from this list to work on

        // These are the currently running tasks.
        std::unordered_map<TaskID, TaskRef> running_tasks;


        // Keep track of mapping from a task to all other task that depend on it
        // used when this task is completed to update the dependent task
        // and schedule them if needed
        std::unordered_map<TaskID, std::unordered_set<TaskRef>> dep_to_tasks;

        void LaunchSleepingThreadOld(int threadId);
        void LaunchSleepingThread2(int threadId);
        bool handle_task_done2(TaskRef task_done); // notify other threads if there is change of state

        void LaunchSleepingThread(int threadId);
        bool handle_task_done(TaskID task_done); // notify other threads if there is change of state

        bool process_dependencies(TaskRef task, std::vector<TaskID>& deps);
        TaskRef getNextRunningTask(int threadId, bool check_only=false);

        // When destroying the thread pool, use these flag to ensure threads exit their
        // loop and cleanup
        bool done; 

        struct paddedMutex{
            std::mutex m;
            char PAD[64];
        };

        // Keep track of all threads launched by this thread pool
        std::vector<std::thread> threads;
        std::vector<std::deque<TaskRef>> task_queue;
        std::vector<paddedMutex> task_queue_mutexes;

        // for waking up the pool of threads
        std::vector<std::condition_variable> condVarThreads;
        std::mutex threads_mtx;       

        // for waking up the main thread waiting on sync
        std::deque<TaskID> task_completed_queue;
        std::condition_variable sync_condVar;
        std::mutex sync_mtx;

};

#endif
