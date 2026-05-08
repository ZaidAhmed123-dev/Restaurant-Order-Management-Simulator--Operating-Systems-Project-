#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
typedef struct {
    int id;
    char customer_name[64];
    char items[256];
    OrderPriority priority;
    time_t order_time;
    time_t start_time;
    time_t completion_time;
    int status;  // 0=pending, 1=being_prepared, 2=completed
    int chef_id; // Which chef is handling it
} Order;

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

// Compare orders: VIP first, then FIFO
static int order_cmp(const Order* a, const Order* b) {
    if (a->priority != b->priority)
        return (b->priority - a->priority);  // VIP first
    return (a->order_time > b->order_time) - (a->order_time < b->order_time);  // FIFO for same priority
}

// Enqueue order (prioritized insertion)
void queue_enqueue(OrderQueue* q, Order order) {
    pthread_mutex_lock(&q->lock);
    
    // Wait if queue is full
    while (q->count >= MAX_QUEUE_SIZE) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }
    
    // Find correct position to insert (maintain priority)
    int insert_pos = q->front;
    int current_pos = q->front;
    int items_checked = 0;
    
    while (items_checked < q->count) {
        if (order_cmp(&order, &q->orders[current_pos]) < 0) {
            insert_pos = current_pos;
            break;
        }
        current_pos = (current_pos + 1) % MAX_QUEUE_SIZE;
        items_checked++;
    }
    
    // Shift elements if inserting in middle
    if (items_checked < q->count) {
        int shift_pos = (q->rear + 1) % MAX_QUEUE_SIZE;
        int current = q->rear;
        
        while (current != (insert_pos - 1 + MAX_QUEUE_SIZE) % MAX_QUEUE_SIZE) {
            q->orders[shift_pos] = q->orders[current];
            shift_pos = current;
            current = (current - 1 + MAX_QUEUE_SIZE) % MAX_QUEUE_SIZE;
        }
        q->rear = shift_pos;
    } else {
        q->rear = (q->rear + 1) % MAX_QUEUE_SIZE;
    }
    
    q->orders[insert_pos] = order;
    q->count++;
    
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

// Dequeue order (FIFO from front)
Order* queue_dequeue(OrderQueue* q) {
    pthread_mutex_lock(&q->lock);
    
    // Wait if queue is empty
    while (q->count == 0) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    
    Order* order = malloc(sizeof(Order));
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
        fprintf(log_file, "[%ld] %s - Order #%d | Customer: %s | Items: %s | Priority: %s\n",
                now, event, order->id, order->customer_name, order->items,
                (order->priority == VIP) ? "VIP" : "NORMAL");
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

void logger_log_completion(Order* order, int chef_id) {
    pthread_mutex_lock(&log_lock);
    if (log_file && order) {
        time_t wait_time = order->start_time - order->order_time;
        time_t prep_time = order->completion_time - order->start_time;
        fprintf(log_file, "[COMPLETED] Order #%d | Chef-%d | Customer: %s | Wait: %ld sec | Prep: %ld sec | Total: %ld sec\n",
                order->id, chef_id, order->customer_name, wait_time, prep_time, wait_time + prep_time);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

void logger_log_queue_status(int queue_size, int active_chefs) {
    pthread_mutex_lock(&log_lock);
    if (log_file) {
        fprintf(log_file, "[STATUS] Queue: %d orders | Active Chefs: %d\n", queue_size, active_chefs);
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
volatile sig_atomic_t running = 1;
static int completed_orders = 0;
static int total_wait_time = 0;
static int total_prep_time = 0;
static pthread_mutex_t stats_lock;
static int order_id_counter = 1;
static pthread_mutex_t id_lock;

int next_order_id() {
    pthread_mutex_lock(&id_lock);
    int id = order_id_counter++;
    pthread_mutex_unlock(&id_lock);
    return id;
}

// Sample menu for random order generation
const char* menu_items[] = {
    "Burger & Fries",
    "Grilled Chicken Sandwich",
    "Spaghetti Carbonara",
    "Caesar Salad",
    "Pizza Margherita",
    "Fish & Chips",
    "Steak Medium Rare",
    "Vegetable Stir-Fry"
};

const char* customer_names[] = {
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank",
    "Grace", "Henry", "Iris", "Jack", "Karen", "Leo",
    "Mia", "Noah", "Olivia", "Peter", "Quinn", "Rachel"
};

// Get random menu item
const char* get_random_item() {
    return menu_items[rand() % 8];
}

// Get random customer name
const char* get_random_customer() {
    return customer_names[rand() % 18];
}

// Print real-time status
void print_status() {
    int q_size = queue_size(&order_queue);
    int active_chefs = NUM_KITCHEN_STATIONS;
    
    printf(COLOR_BOLD COLOR_CYAN "\n╔════════════════════════════════════════╗\n");
    printf("║      RESTAURANT STATUS DASHBOARD       ║\n");
    printf("║════════════════════════════════════════║\n");
    printf("║  Pending Orders: %-25d║\n", q_size);
    printf("║  Active Chefs: %-28d║\n", active_chefs);
    printf("║  Completed Orders: %-22d║\n", completed_orders);
    if (completed_orders > 0) {
        printf("║  Avg Wait Time: %-25ld sec║\n", total_wait_time / completed_orders);
        printf("║  Avg Prep Time: %-25ld sec║\n", total_prep_time / completed_orders);
    }
    printf("╚════════════════════════════════════════╝\n" COLOR_RESET);
}

// Signal handler for graceful shutdown
void handle_sigint(int sig) {
    running = 0;
    printf(COLOR_BOLD COLOR_MAGENTA "\n[INFO] Shutting down restaurant system...\n" COLOR_RESET);
}

// Thread to periodically print status
void* status_monitor(void* arg) {
    while (running) {
        sleep(5);
        if (running) {
            print_status();
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
        new_order.id = next_order_id();
        strncpy(new_order.customer_name, get_random_customer(), sizeof(new_order.customer_name) - 1);
        strncpy(new_order.items, get_random_item(), sizeof(new_order.items) - 1);
        new_order.priority = (rand() % 10 < 2) ? VIP : NORMAL;  // 20% chance of VIP
        new_order.order_time = time(NULL);
        new_order.status = 0;  // pending
        new_order.chef_id = -1;
        
        queue_enqueue(&order_queue, new_order);
        
        printf(COLOR_GREEN "[WAITER-%d] Order #%d placed | Customer: %s | Items: %s | Priority: %s\n" COLOR_RESET,
               waiter_id, new_order.id, new_order.customer_name, new_order.items,
               (new_order.priority == VIP) ? "VIP" : "NORMAL");
        
        logger_log_event("Order Placed", &new_order);
        
        // Waiter takes 2-5 seconds between orders
        sleep(rand() % 4 + 2);
    }
    
    printf(COLOR_YELLOW "[WAITER-%d] Stopping...\n" COLOR_RESET, waiter_id);
    return NULL;
}

// ------------- CHEF THREAD (CONSUMER) ------------- 
void* chef_thread(void* arg) {
    int chef_id = *(int*)arg;
    free(arg);
    
    while (running) {
        // Wait for a kitchen station to be available
        sem_wait(&kitchen_stations);
        
        // Get next order from queue
        Order* order = queue_dequeue(&order_queue);
        
        if (!order) {
            sem_post(&kitchen_stations);
            continue;
        }
        
        order->start_time = time(NULL);
        order->chef_id = chef_id;
        order->status = 1;  // being_prepared
        
        printf(COLOR_MAGENTA "[CHEF-%d] Preparing Order #%d | Customer: %s | Items: %s\n" COLOR_RESET,
               chef_id, order->id, order->customer_name, order->items);
        
        logger_log_event("Preparation Started", order);
        
        // Simulate cooking time (3-8 seconds)
        int prep_time = rand() % 6 + 3;
        sleep(prep_time);
        
        // Mark order as completed
        order->completion_time = time(NULL);
        order->status = 2;  // completed
        
        printf(COLOR_GREEN "[CHEF-%d] Order #%d COMPLETED | Customer: %s\n" COLOR_RESET,
               chef_id, order->id, order->customer_name);
        
        logger_log_completion(order, chef_id);
        
        // Update statistics
        pthread_mutex_lock(&stats_lock);
        completed_orders++;
        total_wait_time += (order->start_time - order->order_time);
        total_prep_time += (order->completion_time - order->start_time);
        pthread_mutex_unlock(&stats_lock);
        
        free(order);
        
        // Release kitchen station for another chef
        sem_post(&kitchen_stations);
    }
    
    printf(COLOR_YELLOW "[CHEF-%d] Stopping...\n" COLOR_RESET, chef_id);
    return NULL;
}

// ------------- MAIN -------------
int main() {
    srand(time(NULL));
    signal(SIGINT, handle_sigint);
    
    // Initialize mutexes
    pthread_mutex_init(&stats_lock, NULL);
    pthread_mutex_init(&id_lock, NULL);
    
    // Initialize queue
    queue_init(&order_queue);
    
    // Initialize logger
    logger_init("restaurant.log");
    
    // Initialize kitchen stations semaphore (limit concurrent chefs)
    sem_init(&kitchen_stations, 0, NUM_KITCHEN_STATIONS);
    
    printf(COLOR_BOLD COLOR_CYAN "\n");
    printf("╔════════════════════════════════════════╗\n");
    printf("║  RESTAURANT ORDER MANAGEMENT SYSTEM   ║\n");
    printf("║══════════════��═════════════════════════║\n");
    printf("║  Waiters: %d | Chefs: %d | Stations: %d ║\n", NUM_WAITERS, NUM_CHEFS, NUM_KITCHEN_STATIONS);
    printf("║  Queue Capacity: %d | Startup Time      ║\n", MAX_QUEUE_SIZE);
    printf("╚════════════════════════════════════════╝\n" COLOR_RESET);
    printf("\nPress Ctrl+C to shutdown system gracefully.\n\n");
    
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
    printf("║  Total Orders Completed: %-15d║\n", completed_orders);
    printf("║  Orders in Queue: %-26d║\n", queue_size(&order_queue));
    if (completed_orders > 0) {
        printf("║  Avg Wait Time: %-27ld sec║\n", total_wait_time / completed_orders);
        printf("║  Avg Prep Time: %-27ld sec║\n", total_prep_time / completed_orders);
        printf("║  Avg Total Time: %-26ld sec║\n", (total_wait_time + total_prep_time) / completed_orders);
    }
    printf("╚════════════════════════════════════════╝\n" COLOR_RESET);
    
    // Cleanup
    logger_close();
    printf(COLOR_BOLD COLOR_GREEN "System shutdown complete. Check 'restaurant.log' for detailed logs.\n" COLOR_RESET);
    
    return 0;
}
