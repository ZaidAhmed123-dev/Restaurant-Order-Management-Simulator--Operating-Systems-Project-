#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

// ANSI color codes for beautification
#define COLOR_RESET   "\033[0m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_RED     "\033[31m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_CYAN    "\033[36m"
#define COLOR_MAGENTA "\033[35m"
#define COLOR_BOLD    "\033[1m"
#define COLOR_BLUE    "\033[34m"

// ------------- CONSTANTS & ENUMS ------------
#define MAX_QUEUE_SIZE 20
#define NUM_WAITERS 3
#define NUM_CHEFS 4
#define NUM_KITCHEN_STATIONS 2
#define MAX_ORDERS 100

typedef enum { NORMAL = 0, VIP = 1 } OrderPriority;

// ------------- ORDER STRUCT ------------
static volatile sig_atomic_t running = 1;

typedef struct {
    int id;
    char customer_name[64];
    char items[128];
    int arrival_time;
    int prep_time;
    OrderPriority priority;
    time_t enqueue_time;
    time_t start_time;
    time_t completion_time;
    int status;  // 0=pending, 1=being_prepared, 2=completed
    int chef_id; // Which chef is handling it
} Order;

typedef enum { SCHED_FCFS = 0, SCHED_PRIORITY = 1, SCHED_SJF = 2 } SchedulingPolicy;

// ------------- CIRCULAR QUEUE -----------
typedef struct {
    Order orders[MAX_QUEUE_SIZE];
    int front;
    int rear;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;  // Signal when queue has orders
    pthread_cond_t not_full;   // Signal when queue has space
} OrderQueue;

void queue_init(OrderQueue* q) {
    q->front = 0;
    q->rear = -1;
    q->count = 0;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static int order_cmp(const Order* a, const Order* b);

// Enqueue order (prioritized insertion)
void queue_enqueue(OrderQueue* q, Order order) {
    pthread_mutex_lock(&q->lock);
    
    // Wait if queue is full
    while (q->count >= MAX_QUEUE_SIZE && running) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }
    if (!running) {
        pthread_mutex_unlock(&q->lock);
        return;
    }
    
    // Find correct position to insert (maintain priority)
    int insert_index = q->count;
    for (int i = 0; i < q->count; i++) {
        int pos = (q->front + i) % MAX_QUEUE_SIZE;
        if (order_cmp(&order, &q->orders[pos]) < 0) {
            insert_index = i;
            break;
        }
    }

    // Shift elements to make room for the insert position
    for (int i = q->count; i > insert_index; i--) {
        int to_pos = (q->front + i) % MAX_QUEUE_SIZE;
        int from_pos = (q->front + i - 1) % MAX_QUEUE_SIZE;
        q->orders[to_pos] = q->orders[from_pos];
    }

    q->orders[(q->front + insert_index) % MAX_QUEUE_SIZE] = order;
    q->count++;
    q->rear = (q->front + q->count - 1) % MAX_QUEUE_SIZE;
    
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

// Dequeue order (FIFO from front)
Order* queue_dequeue(OrderQueue* q) {
    pthread_mutex_lock(&q->lock);
    
    // Wait if queue is empty
    while (q->count == 0 && running) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    if (q->count == 0) {
        pthread_mutex_unlock(&q->lock);
        return NULL;
    }
    
    Order* order = malloc(sizeof(Order));
    if (!order) {
        pthread_mutex_unlock(&q->lock);
        return NULL;
    }
    *order = q->orders[q->front];
    q->front = (q->front + 1) % MAX_QUEUE_SIZE;
    q->count--;
    
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    
    return order;
}

int queue_size(OrderQueue* q) {
    pthread_mutex_lock(&q->lock);
    int size = q->count;
    pthread_mutex_unlock(&q->lock);
    return size;
}

int queue_is_empty(OrderQueue* q) {
    pthread_mutex_lock(&q->lock);
    int empty = (q->count == 0);
    pthread_mutex_unlock(&q->lock);
    return empty;
}

// ------------- LOGGER -------------
static FILE* log_file = NULL;
static pthread_mutex_t log_lock;

void logger_init(const char* filename) {
    pthread_mutex_init(&log_lock, NULL);
    log_file = fopen(filename, "a");
    if (log_file) {
        fprintf(log_file, "\n=== Restaurant Order Management System Started ===\n");
        fprintf(log_file, "Timestamp: %ld\n\n", time(NULL));
        fflush(log_file);
    }
}

void logger_log_event(const char* event, Order* order) {
    pthread_mutex_lock(&log_lock);
    if (log_file && order) {
        time_t now = time(NULL);
        fprintf(log_file, "[INFO][%ld] %s - Order #%d | Customer: %s | Items: %s | Priority: %s | Arrival: %d | Prep: %d\n",
                now, event, order->id, order->customer_name, order->items,
            (order->priority == VIP) ? "VIP" : "NORMAL", order->arrival_time, order->prep_time);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

void logger_log_completion(Order* order, int chef_id) {
    pthread_mutex_lock(&log_lock);
    if (log_file && order) {
    time_t wait_time = order->start_time - order->enqueue_time;
        time_t prep_time = order->completion_time - order->start_time;
        fprintf(log_file, "[INFO][COMPLETED] Order #%d | Chef-%d | Customer: %s | Wait: %ld sec | Prep: %ld sec | Total: %ld sec\n",
                order->id, chef_id, order->customer_name, wait_time, prep_time, wait_time + prep_time);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

void logger_log_queue_status(int queue_size, int active_chefs) {
    pthread_mutex_lock(&log_lock);
    if (log_file) {
        fprintf(log_file, "[INFO][STATUS] Queue: %d orders | Active Chefs: %d\n", queue_size, active_chefs);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

void logger_close() {
    pthread_mutex_lock(&log_lock);
    if (log_file) {
        fprintf(log_file, "\n=== Restaurant System Shutdown ===\n");
        fclose(log_file);
        log_file = NULL;
    }
    pthread_mutex_unlock(&log_lock);
}

// ------------- GLOBALS ------------
OrderQueue order_queue;
sem_t kitchen_stations;
static SchedulingPolicy scheduling_policy = SCHED_FCFS;
static const int AGING_THRESHOLD_SEC = 20;
static long int total_wait_time = 0;
static long int total_prep_time = 0;
static long int completed_orders = 0;
static int active_chefs = 0;
static int total_orders = 0;
static time_t simulation_start_time = 0;
static pthread_mutex_t stats_lock;

static Order pending_orders[MAX_ORDERS];
static int pending_index = 0;
static pthread_mutex_t pending_lock;

static void read_line(const char* prompt, char* buffer, size_t size) {
    if (prompt) {
        printf("%s", prompt);
        fflush(stdout);
    }
    if (fgets(buffer, (int)size, stdin)) {
        size_t len = strlen(buffer);
        if (len > 0 && buffer[len - 1] == '\n') {
            buffer[len - 1] = '\0';
        }
    } else {
        buffer[0] = '\0';
    }
}

static int read_int(const char* prompt) {
    char line[64];
    read_line(prompt, line, sizeof(line));
    return (int)strtol(line, NULL, 10);
}

static int equals_ignore_case(const char* a, const char* b) {
    if (!a || !b) {
        return 0;
    }
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) {
            return 0;
        }
        a++;
        b++;
    }
    return (*a == '\0' && *b == '\0');
}

static OrderPriority parse_priority(const char* input) {
    if (equals_ignore_case(input, "VIP")) {
        return VIP;
    }
    return NORMAL;
}

static OrderPriority effective_priority(const Order* order) {
    if (!order) {
        return NORMAL;
    }
    if (order->priority == VIP) {
        return VIP;
    }
    if (order->enqueue_time > 0) {
        time_t now = time(NULL);
        if ((int)difftime(now, order->enqueue_time) > AGING_THRESHOLD_SEC) {
            return VIP;
        }
    }
    return order->priority;
}

// Compare orders according to scheduling policy with aging support
static int order_cmp(const Order* a, const Order* b) {
    if (scheduling_policy == SCHED_PRIORITY) {
        OrderPriority pa = effective_priority(a);
        OrderPriority pb = effective_priority(b);
        if (pa != pb) {
            return (pb - pa);
        }
    } else if (scheduling_policy == SCHED_SJF) {
        if (a->prep_time != b->prep_time) {
            return (a->prep_time - b->prep_time);
        }
    }

    if (a->arrival_time != b->arrival_time) {
        return (a->arrival_time - b->arrival_time);
    }
    return (a->id - b->id);
}

static SchedulingPolicy read_scheduling_policy(void) {
    printf("\n===== SCHEDULING POLICY =====\n");
    printf("1. FCFS\n");
    printf("2. Priority\n");
    printf("3. Shortest Job First (SJF)\n");
    int choice = read_int("Select policy: ");

    switch (choice) {
        case 2:
            return SCHED_PRIORITY;
        case 3:
            return SCHED_SJF;
        case 1:
        default:
            return SCHED_FCFS;
    }
}

static void read_orders_input(void) {
    printf("\n===== ORDER INPUT =====\n");
    total_orders = read_int("Enter total orders: ");
    if (total_orders < 1) {
        total_orders = 1;
    }
    if (total_orders > MAX_ORDERS) {
        printf("[WARNING] Max orders capped at %d.\n", MAX_ORDERS);
        total_orders = MAX_ORDERS;
    }

    for (int i = 0; i < total_orders; i++) {
        char line[128];

        printf("\nOrder #%d\n", i + 1);
        read_line("Customer Name: ", pending_orders[i].customer_name, sizeof(pending_orders[i].customer_name));
        read_line("Food Item: ", pending_orders[i].items, sizeof(pending_orders[i].items));
        read_line("Priority (VIP/NORMAL): ", line, sizeof(line));
        pending_orders[i].priority = parse_priority(line);
        pending_orders[i].arrival_time = read_int("Arrival Time (sec): ");
        pending_orders[i].prep_time = read_int("Prep Time (sec): ");

        if (pending_orders[i].arrival_time < 0) {
            pending_orders[i].arrival_time = 0;
        }
        if (pending_orders[i].prep_time < 1) {
            pending_orders[i].prep_time = 1;
        }

        pending_orders[i].id = i + 1;
        pending_orders[i].enqueue_time = 0;
        pending_orders[i].start_time = 0;
        pending_orders[i].completion_time = 0;
        pending_orders[i].status = 0;
        pending_orders[i].chef_id = -1;
    }
}

// Print real-time status
void print_status() {
    int q_size = queue_size(&order_queue);
    long completed = 0;
    long wait_time = 0;
    long prep_time = 0;
    int active = 0;
    time_t elapsed = 0;

    pthread_mutex_lock(&stats_lock);
    completed = completed_orders;
    wait_time = total_wait_time;
    prep_time = total_prep_time;
    active = active_chefs;
    pthread_mutex_unlock(&stats_lock);
    elapsed = time(NULL) - simulation_start_time;
    
    printf(COLOR_BOLD COLOR_CYAN "\n╔════════════════════════════════════════╗\n");
    printf("║      RESTAURANT STATUS DASHBOARD       ║\n");
    printf("║════════════════════════════════════════║\n");
    printf("║  Pending Orders: %-25d║\n", q_size);
    printf("║  Active Chefs: %-28d║\n", active);
    printf("║  Completed Orders: %-22ld║\n", completed);
    if (completed > 0) {
        printf("║  Avg Wait Time: %-25ld sec║\n", wait_time / completed);
        printf("║  Avg Prep Time: %-25ld sec║\n", prep_time / completed);
    }
    if (elapsed > 0) {
        printf("║  Throughput: %-26.2f║\n", (double)completed / (double)elapsed);
    }
    printf("╚════════════════════════════════════════╝\n" COLOR_RESET);
}

// Signal handler for graceful shutdown
void handle_sigint(int sig) {
    (void)sig;
    running = 0;
    printf(COLOR_BOLD COLOR_MAGENTA "\n[INFO] Shutting down restaurant system...\n" COLOR_RESET);

    pthread_mutex_lock(&order_queue.lock);
    pthread_cond_broadcast(&order_queue.not_empty);
    pthread_cond_broadcast(&order_queue.not_full);
    pthread_mutex_unlock(&order_queue.lock);

    for (int i = 0; i < NUM_CHEFS; i++) {
        sem_post(&kitchen_stations);
    }
}

// Thread to periodically print status
void* status_monitor(void* arg) {
    while (running) {
        sleep(5);
        if (running) {
            print_status();
            pthread_mutex_lock(&stats_lock);
            int active = active_chefs;
            pthread_mutex_unlock(&stats_lock);
            logger_log_queue_status(queue_size(&order_queue), active);
        }
    }
    return NULL;
}

// ------------- WAITER THREAD (PRODUCER) ------------- 
void* waiter_thread(void* arg) {
    int waiter_id = *(int*)arg;
    free(arg);
    
    while (running) {
        Order new_order;
        int has_order = 0;

        pthread_mutex_lock(&pending_lock);
        if (pending_index < total_orders) {
            new_order = pending_orders[pending_index++];
            has_order = 1;
        }
        pthread_mutex_unlock(&pending_lock);

        if (!has_order) {
            break;
        }

        time_t target_time = simulation_start_time + new_order.arrival_time;
        time_t now = time(NULL);
        if (target_time > now) {
            sleep((unsigned int)(target_time - now));
        }

        if (!running) {
            break;
        }

        new_order.enqueue_time = time(NULL);
        new_order.status = 0;
        new_order.chef_id = -1;

        queue_enqueue(&order_queue, new_order);

        printf(COLOR_GREEN "[WAITER-%d] Added Order #%d to queue\n" COLOR_RESET,
               waiter_id, new_order.id);

        logger_log_event("Order Enqueued", &new_order);
    }
    
    printf(COLOR_YELLOW "[WAITER-%d] Stopping...\n" COLOR_RESET, waiter_id);
    return NULL;
}

// ------------- CHEF THREAD (CONSUMER) ------------- 
void* chef_thread(void* arg) {
    int chef_id = *(int*)arg;
    free(arg);
    
    while (running || !queue_is_empty(&order_queue)) {
        // Wait for a kitchen station to be available
        sem_wait(&kitchen_stations);
        if (!running) {
            sem_post(&kitchen_stations);
            if (queue_is_empty(&order_queue)) {
                break;
            }
        }
        
        // Get next order from queue
        Order* order = queue_dequeue(&order_queue);
        
        if (!order) {
            sem_post(&kitchen_stations);
            if (!running) {
                break;
            }
            continue;
        }
        
        order->start_time = time(NULL);
        order->chef_id = chef_id;
        order->status = 1;  // being_prepared

        pthread_mutex_lock(&stats_lock);
        active_chefs++;
        pthread_mutex_unlock(&stats_lock);
        
        printf(COLOR_MAGENTA "[CHEF-%d] Preparing Order #%d | Customer: %s | Items: %s\n" COLOR_RESET,
               chef_id, order->id, order->customer_name, order->items);
        
        logger_log_event("Preparation Started", order);
        
        // Simulate cooking time based on requested prep time
        sleep((unsigned int)order->prep_time);
        
        // Mark order as completed
        order->completion_time = time(NULL);
        order->status = 2;  // completed
        
        printf(COLOR_GREEN "[CHEF-%d] Order #%d COMPLETED | Customer: %s\n" COLOR_RESET,
               chef_id, order->id, order->customer_name);
        
        logger_log_completion(order, chef_id);
        
        // Update statistics
        int should_shutdown = 0;
        pthread_mutex_lock(&stats_lock);
        completed_orders++;
        active_chefs--;
        total_wait_time += (order->start_time - order->enqueue_time);
        total_prep_time += (order->completion_time - order->start_time);
        if (completed_orders >= total_orders) {
            running = 0;
            should_shutdown = 1;
        }
        pthread_mutex_unlock(&stats_lock);

        if (should_shutdown) {
            pthread_mutex_lock(&order_queue.lock);
            pthread_cond_broadcast(&order_queue.not_empty);
            pthread_cond_broadcast(&order_queue.not_full);
            pthread_mutex_unlock(&order_queue.lock);
            for (int i = 0; i < NUM_CHEFS; i++) {
                sem_post(&kitchen_stations);
            }
        }
        
        free(order);
        
        // Release kitchen station for another chef
        sem_post(&kitchen_stations);

        if (!running && queue_is_empty(&order_queue)) {
            break;
        }
    }
    
    printf(COLOR_YELLOW "[CHEF-%d] Stopping...\n" COLOR_RESET, chef_id);
    return NULL;
}

// ------------- MAIN -------------
int main() {
    signal(SIGINT, handle_sigint);
    
    // Initialize mutexes
    pthread_mutex_init(&stats_lock, NULL);
    pthread_mutex_init(&pending_lock, NULL);
    
    // Initialize queue
    queue_init(&order_queue);

    // Read input and scheduling policy
    read_orders_input();
    scheduling_policy = read_scheduling_policy();
    simulation_start_time = time(NULL);
    
    // Initialize logger
    logger_init("restaurant.log");
    
    // Initialize kitchen stations semaphore (limit concurrent chefs)
    sem_init(&kitchen_stations, 0, NUM_KITCHEN_STATIONS);
    
    printf(COLOR_BOLD COLOR_CYAN "\n");
    printf("╔════════════════════════════════════════╗\n");
    printf("║  RESTAURANT ORDER MANAGEMENT SYSTEM   ║\n");
    printf("║══════════════��═════════════════════════║\n");
    printf("║  Waiters: %d | Chefs: %d | Stations: %d ║\n", NUM_WAITERS, NUM_CHEFS, NUM_KITCHEN_STATIONS);
    printf("║  Queue Capacity: %d | Orders: %-8d║\n", MAX_QUEUE_SIZE, total_orders);
    printf("╚════════════════════════════════════════╝\n" COLOR_RESET);
    printf("\nStarting Restaurant Simulation...\n\n");
    
    // Create waiter threads (producers)
    pthread_t waiter_threads[NUM_WAITERS];
    for (int i = 0; i < NUM_WAITERS; i++) {
        int* waiter_id = malloc(sizeof(int));
        *waiter_id = i + 1;
        pthread_create(&waiter_threads[i], NULL, waiter_thread, waiter_id);
    }
    
    // Create chef threads (consumers)
    pthread_t chef_threads[NUM_CHEFS];
    for (int i = 0; i < NUM_CHEFS; i++) {
        int* chef_id = malloc(sizeof(int));
        *chef_id = i + 1;
        pthread_create(&chef_threads[i], NULL, chef_thread, chef_id);
    }
    
    // Create status monitor thread
    pthread_t status_thread;
    pthread_create(&status_thread, NULL, status_monitor, NULL);
    
    // Wait for all threads to complete (they run until SIGINT)
    for (int i = 0; i < NUM_WAITERS; i++) {
        pthread_join(waiter_threads[i], NULL);
    }
    
    for (int i = 0; i < NUM_CHEFS; i++) {
        pthread_join(chef_threads[i], NULL);
    }
    
    pthread_join(status_thread, NULL);
    
    // Final statistics
    printf("\n");
    printf(COLOR_BOLD COLOR_GREEN "╔════════════════════════════════════════╗\n");
    printf("║       FINAL STATISTICS                 ║\n");
    printf("║════════════════════════════════════════║\n");
    long completed = 0;
    long wait_time = 0;
    long prep_time = 0;
    int remaining = 0;
    time_t elapsed = 0;

    pthread_mutex_lock(&stats_lock);
    completed = completed_orders;
    wait_time = total_wait_time;
    prep_time = total_prep_time;
    pthread_mutex_unlock(&stats_lock);
    remaining = queue_size(&order_queue);
    elapsed = time(NULL) - simulation_start_time;

    printf("║  Total Orders Completed: %-15ld║\n", completed);
    printf("║  Orders in Queue: %-26d║\n", remaining);
    if (completed > 0) {
        printf("║  Avg Wait Time: %-27ld sec║\n", wait_time / completed);
        printf("║  Avg Prep Time: %-27ld sec║\n", prep_time / completed);
        printf("║  Avg Total Time: %-26ld sec║\n", (wait_time + prep_time) / completed);
    }
    if (elapsed > 0) {
        printf("║  Throughput: %-29.2f║\n", (double)completed / (double)elapsed);
    }
    printf("╚════════════════════════════════════════╝\n" COLOR_RESET);
    
    // Cleanup
    logger_close();
    printf(COLOR_BOLD COLOR_GREEN "System shutdown complete. Check 'restaurant.log' for detailed logs.\n" COLOR_RESET);
    
    return 0;
}
