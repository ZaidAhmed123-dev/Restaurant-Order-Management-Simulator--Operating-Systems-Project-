#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

#define COLOR_RESET   "\033[0m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_RED     "\033[31m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_CYAN    "\033[36m"
#define COLOR_MAGENTA "\033[35m"
#define COLOR_BOLD    "\033[1m"

#define MAX_QUEUE_SIZE 20
#define NUM_WAITERS 3
#define NUM_CHEFS 4
#define NUM_KITCHEN_STATIONS 2
#define MAX_ORDERS 100
#define RR_TIME_SLICE_SEC 2

#define AGING_THRESHOLD_SEC 20

typedef enum {
    NORMAL = 0,
    VIP = 1
} OrderPriority;

typedef enum {
    POLICY_FCFS = 0,
    POLICY_PRIORITY = 1,
    POLICY_RR = 2,
    POLICY_SJF = 3,
    POLICY_MLQ = 4
} SchedulingPolicy;

typedef struct {
    int id;
    char customer_name[64];
    char items[128];
    int arrival_time;
    int prep_time;
    int remaining_prep_time;
    OrderPriority priority;
    time_t enqueue_time;
    time_t first_enqueue_time;
    time_t start_time;
    time_t completion_time;
    int status;
    int chef_id;
    int starvation_reported;
} Order;

typedef struct {
    Order orders[MAX_QUEUE_SIZE];
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} OrderQueue;

static volatile sig_atomic_t running = 1;
static volatile sig_atomic_t kitchen_paused = 0;
static volatile sig_atomic_t orders_closed = 0;

static OrderQueue order_queue;
static OrderQueue vip_queue;
static OrderQueue normal_queue;

static sem_t kitchen_stations;
static SchedulingPolicy scheduling_policy = POLICY_FCFS;

static long int total_wait_time = 0;
static long int total_turnaround_time = 0;
static long int total_prep_time = 0;
static long int completed_orders = 0;
static long int starvation_count = 0;
static int active_chefs = 0;
static int total_orders = 0;
static time_t simulation_start_time = 0;

static Order pending_orders[MAX_ORDERS];
static int pending_index = 0;

static pthread_mutex_t stats_lock;
static pthread_mutex_t pending_lock;
static pthread_cond_t pending_available;
static pthread_mutex_t kitchen_pause_lock;
static pthread_cond_t kitchen_resume_cond;
static pthread_mutex_t log_lock;
static FILE *log_file = NULL;

static void wake_all_threads(void);
static void maybe_finalize_shutdown(void);
static void print_status(void);

static void read_line(const char *prompt, char *buffer, size_t size) {
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

static int read_int(const char *prompt) {
    char line[64];
    read_line(prompt, line, sizeof(line));
    return (int)strtol(line, NULL, 10);
}

static int equals_ignore_case(const char *a, const char *b) {
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

static const char *priority_name(OrderPriority priority) {
    return (priority == VIP) ? "VIP" : "NORMAL";
}

static const char *scheduling_name(SchedulingPolicy policy) {
    switch (policy) {
        case POLICY_FCFS: return "FCFS";
        case POLICY_PRIORITY: return "Priority";
        case POLICY_RR: return "Round Robin";
        case POLICY_SJF: return "Shortest Job First";
        case POLICY_MLQ: return "Multilevel Queue";
        default: return "Unknown";
    }
}

static OrderPriority parse_priority(const char *input) {
    if (equals_ignore_case(input, "VIP") || equals_ignore_case(input, "1")) {
        return VIP;
    }
    return NORMAL;
}

static void queue_init(OrderQueue *queue) {
    queue->count = 0;
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);
}

static void queue_append_locked(OrderQueue *queue, const Order *order) {
    if (queue->count < MAX_QUEUE_SIZE) {
        queue->orders[queue->count++] = *order;
    }
}

static void queue_remove_index_locked(OrderQueue *queue, int index) {
    for (int i = index; i < queue->count - 1; i++) {
        queue->orders[i] = queue->orders[i + 1];
    }
    if (queue->count > 0) {
        queue->count--;
    }
}

static int queue_size_locked(OrderQueue *queue) {
    return queue->count;
}

static int pending_remaining(void) {
    int remaining;
    pthread_mutex_lock(&pending_lock);
    remaining = total_orders - pending_index;
    if (remaining < 0) {
        remaining = 0;
    }
    pthread_mutex_unlock(&pending_lock);
    return remaining;
}

static int all_queue_sizes(void) {
    int size = 0;
    pthread_mutex_lock(&order_queue.lock);
    size += queue_size_locked(&order_queue);
    pthread_mutex_unlock(&order_queue.lock);

    pthread_mutex_lock(&normal_queue.lock);
    size += queue_size_locked(&normal_queue);
    pthread_mutex_unlock(&normal_queue.lock);

    pthread_mutex_lock(&vip_queue.lock);
    size += queue_size_locked(&vip_queue);
    pthread_mutex_unlock(&vip_queue.lock);

    return size;
}

static void logger_init(const char *filename) {
    pthread_mutex_init(&log_lock, NULL);
    log_file = fopen(filename, "a");
    if (log_file) {
        fprintf(log_file, "\n=== Restaurant Order Management System Started ===\n");
        fprintf(log_file, "Timestamp: %ld\n\n", time(NULL));
        fflush(log_file);
    }
}

static void logger_log_event(const char *event, Order *order) {
    pthread_mutex_lock(&log_lock);
    if (log_file && order) {
        fprintf(log_file,
                "[INFO][%ld] %s - Order #%d | Customer: %s | Items: %s | Priority: %s | Arrival: %d | Prep: %d | Remaining: %d\n",
                (long)time(NULL),
                event,
                order->id,
                order->customer_name,
                order->items,
                priority_name(order->priority),
                order->arrival_time,
                order->prep_time,
                order->remaining_prep_time);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

static void logger_log_completion(Order *order, int chef_id, long queue_wait, long turnaround) {
    pthread_mutex_lock(&log_lock);
    if (log_file && order) {
        fprintf(log_file,
                "[INFO][COMPLETED] Order #%d | Chef-%d | Customer: %s | Queue Wait: %ld sec | Turnaround: %ld sec | Prep Done: %d sec\n",
                order->id,
                chef_id,
                order->customer_name,
                queue_wait,
                turnaround,
                order->prep_time);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

static void logger_log_queue_status(int queue_size_value, int active) {
    pthread_mutex_lock(&log_lock);
    if (log_file) {
        fprintf(log_file, "[INFO][STATUS] Queue: %d orders | Active Chefs: %d\n", queue_size_value, active);
        fflush(log_file);
    }
    pthread_mutex_unlock(&log_lock);
}

static void logger_close(void) {
    pthread_mutex_lock(&log_lock);
    if (log_file) {
        fprintf(log_file, "\n=== Restaurant System Shutdown ===\n");
        fclose(log_file);
        log_file = NULL;
    }
    pthread_mutex_unlock(&log_lock);
}

static void apply_aging(Order *order) {
    if (!order || order->priority == VIP || order->enqueue_time <= 0 || order->starvation_reported) {
        return;
    }

    time_t now = time(NULL);
    if ((int)difftime(now, order->enqueue_time) > AGING_THRESHOLD_SEC) {
        order->priority = VIP;
        order->starvation_reported = 1;
        pthread_mutex_lock(&stats_lock);
        starvation_count++;
        pthread_mutex_unlock(&stats_lock);
    }
}

static int order_cmp(Order *a, Order *b) {
    if (!a || !b) {
        return 0;
    }

    apply_aging(a);
    apply_aging(b);

    if (scheduling_policy == POLICY_PRIORITY) {
        if (a->priority != b->priority) {
            return (int)b->priority - (int)a->priority;
        }
    } else if (scheduling_policy == POLICY_SJF) {
        if (a->remaining_prep_time != b->remaining_prep_time) {
            return a->remaining_prep_time - b->remaining_prep_time;
        }
    }

    if (a->arrival_time != b->arrival_time) {
        return a->arrival_time - b->arrival_time;
    }
    return a->id - b->id;
}

static void queue_enqueue(OrderQueue *queue, Order order) {
    pthread_mutex_lock(&queue->lock);

    while (queue->count >= MAX_QUEUE_SIZE && running) {
        pthread_cond_wait(&queue->not_full, &queue->lock);
    }

    if (!running) {
        pthread_mutex_unlock(&queue->lock);
        return;
    }

    queue_append_locked(queue, &order);
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->lock);
}

static Order *queue_dequeue_best(OrderQueue *queue, SchedulingPolicy policy) {
    pthread_mutex_lock(&queue->lock);

    while (queue->count == 0 && running) {
        pthread_cond_wait(&queue->not_empty, &queue->lock);
    }

    if (queue->count == 0) {
        pthread_mutex_unlock(&queue->lock);
        return NULL;
    }

    int best_index = 0;
    if (policy != POLICY_FCFS && policy != POLICY_RR) {
        for (int i = 1; i < queue->count; i++) {
            if (order_cmp(&queue->orders[i], &queue->orders[best_index]) < 0) {
                best_index = i;
            }
        }
    }

    Order *order = malloc(sizeof(Order));
    if (!order) {
        pthread_mutex_unlock(&queue->lock);
        return NULL;
    }

    *order = queue->orders[best_index];
    queue_remove_index_locked(queue, best_index);
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->lock);
    return order;
}

static void print_order_line(const Order *order) {
    if (!order) {
        return;
    }

    printf("  #%d | %-16s | %-18s | %-6s | Prep: %-3d | Remaining: %-3d | Arrival: %-3d\n",
           order->id,
           order->customer_name,
           order->items,
           priority_name(order->priority),
           order->prep_time,
           order->remaining_prep_time,
           order->arrival_time);
}

static void print_queue_snapshot(const char *label, OrderQueue *queue) {
    pthread_mutex_lock(&queue->lock);
    printf("%s (%d orders)\n", label, queue->count);
    for (int i = 0; i < queue->count; i++) {
        print_order_line(&queue->orders[i]);
    }
    if (queue->count == 0) {
        printf("  [empty]\n");
    }
    pthread_mutex_unlock(&queue->lock);
}

static void print_feature_overview(void) {
    printf("\n%s%sFeature Overview%s\n", COLOR_BOLD, COLOR_CYAN, COLOR_RESET);
    printf("- Manual order entry before the auto-simulation starts.\n");
    printf("- Interactive control menu during the running simulation.\n");
    printf("- Producer-consumer threading with waiter producers and chef consumers.\n");
    printf("- FCFS scheduling for simple first-come, first-served handling.\n");
    printf("- Priority scheduling with VIP orders and starvation-preventing aging.\n");
    printf("- Round Robin scheduling with time slicing for preemptive service.\n");
    printf("- Shortest Job First using estimated prep time to favor short orders.\n");
    printf("- Multilevel Queue scheduling with separate VIP and normal queues.\n");
    printf("- Pause and resume controls for the kitchen.\n");
    printf("- Live queue inspection and periodic status reporting.\n");
    printf("- Analytics for throughput, turnaround, queue wait, utilization, and starvation.\n");
    printf("- Detailed logging to restaurant.log for events and completions.\n");
}

static void print_banner(void) {
    printf("%s%s\n", COLOR_BOLD, COLOR_CYAN);
    printf("==========================================\n");
    printf("  RESTAURANT ORDER MANAGEMENT SYSTEM\n");
    printf("==========================================\n");
    printf(" Waiters: %d | Chefs: %d | Stations: %d\n", NUM_WAITERS, NUM_CHEFS, NUM_KITCHEN_STATIONS);
    printf(" Queue Capacity: %d\n", MAX_QUEUE_SIZE);
    printf("==========================================\n%s", COLOR_RESET);
}

static void read_order_details(Order *order, int id, int runtime_order) {
    char line[128];

    memset(order, 0, sizeof(*order));
    order->id = id;
    read_line("Customer Name: ", order->customer_name, sizeof(order->customer_name));
    read_line("Items: ", order->items, sizeof(order->items));
    read_line("Priority (VIP/NORMAL): ", line, sizeof(line));
    order->priority = parse_priority(line);
    order->prep_time = read_int("Prep Time (seconds): ");

    if (order->prep_time < 1) {
        order->prep_time = 1;
    }

    /* arrival_time will be set by caller: initial batches get auto-assigned, runtime orders are immediate */
    order->remaining_prep_time = order->prep_time;
    order->enqueue_time = 0;
    order->first_enqueue_time = 0;
    order->start_time = 0;
    order->completion_time = 0;
    order->status = 0;
    order->chef_id = -1;
    order->starvation_reported = 0;
}

static void prompt_for_initial_orders(void) {
    int count = read_int("How many orders do you want to enter now? ");
    if (count < 0) {
        count = 0;
    }
    if (count > MAX_ORDERS) {
        printf("[WARNING] Maximum order count capped at %d.\n", MAX_ORDERS);
        count = MAX_ORDERS;
    }

    pthread_mutex_lock(&pending_lock);
    int next_arrival = 0;
    for (int i = 0; i < count; i++) {
        printf("\n--- Order #%d ---\n", total_orders + 1);
        read_order_details(&pending_orders[total_orders], total_orders + 1, 0);
        /* auto-assign arrival times spaced by 1 second to avoid asking the user */
        pending_orders[total_orders].arrival_time = next_arrival++;
        total_orders++;
    }
    pthread_mutex_unlock(&pending_lock);
}

static SchedulingPolicy read_scheduling_policy(void) {
    printf("\nSelect Scheduling Algorithm:\n");
    printf("1. FCFS\n");
    printf("2. Priority\n");
    printf("3. Round Robin\n");
    printf("4. Shortest Job First\n");
    printf("5. Multilevel Queue\n");

    switch (read_int("Choice: ")) {
        case 2: return POLICY_PRIORITY;
        case 3: return POLICY_RR;
        case 4: return POLICY_SJF;
        case 5: return POLICY_MLQ;
        case 1:
        default:
            return POLICY_FCFS;
    }
}

static void pause_kitchen(void) {
    pthread_mutex_lock(&kitchen_pause_lock);
    kitchen_paused = 1;
    pthread_mutex_unlock(&kitchen_pause_lock);
    printf(COLOR_YELLOW "[INFO] Kitchen paused.\n" COLOR_RESET);
}

static void resume_kitchen(void) {
    pthread_mutex_lock(&kitchen_pause_lock);
    kitchen_paused = 0;
    pthread_cond_broadcast(&kitchen_resume_cond);
    pthread_mutex_unlock(&kitchen_pause_lock);
    printf(COLOR_GREEN "[INFO] Kitchen resumed.\n" COLOR_RESET);
}

static void wait_if_paused(void) {
    pthread_mutex_lock(&kitchen_pause_lock);
    while (kitchen_paused && running) {
        pthread_cond_wait(&kitchen_resume_cond, &kitchen_pause_lock);
    }
    pthread_mutex_unlock(&kitchen_pause_lock);
}

static void enqueue_ready_order(Order order) {
    order.enqueue_time = time(NULL);
    if (order.first_enqueue_time == 0) {
        order.first_enqueue_time = order.enqueue_time;
    }

    if (scheduling_policy == POLICY_MLQ) {
        if (order.priority == VIP) {
            queue_enqueue(&vip_queue, order);
        } else {
            queue_enqueue(&normal_queue, order);
        }
    } else {
        queue_enqueue(&order_queue, order);
    }
}

static void add_order_to_system(int runtime_order) {
    Order new_order;

    pthread_mutex_lock(&pending_lock);
    if (total_orders >= MAX_ORDERS) {
        pthread_mutex_unlock(&pending_lock);
        printf(COLOR_RED "[ERROR] Maximum order limit reached.\n" COLOR_RESET);
        return;
    }

    printf("\n--- New Runtime Order #%d ---\n", total_orders + 1);
    read_order_details(&new_order, total_orders + 1, runtime_order);
    pending_orders[total_orders] = new_order;
    total_orders++;

    if (runtime_order) {
        pending_orders[total_orders - 1].arrival_time = 0;
    }

    pthread_cond_signal(&pending_available);
    pthread_mutex_unlock(&pending_lock);

    printf(COLOR_GREEN "[INFO] Order added successfully.\n" COLOR_RESET);
}

static void maybe_promote_aged_orders_for_mlq(void) {
    if (scheduling_policy != POLICY_MLQ) {
        return;
    }

    Order promoted[MAX_QUEUE_SIZE];
    int promoted_count = 0;

    pthread_mutex_lock(&normal_queue.lock);
    for (int i = 0; i < normal_queue.count; ) {
        Order *order = &normal_queue.orders[i];
        if (order->enqueue_time > 0 && !order->starvation_reported &&
            (int)difftime(time(NULL), order->enqueue_time) > AGING_THRESHOLD_SEC) {
            promoted[promoted_count++] = *order;
            queue_remove_index_locked(&normal_queue, i);
            continue;
        }
        i++;
    }
    pthread_mutex_unlock(&normal_queue.lock);

    for (int i = 0; i < promoted_count; i++) {
        promoted[i].priority = VIP;
        promoted[i].starvation_reported = 1;
        pthread_mutex_lock(&stats_lock);
        starvation_count++;
        pthread_mutex_unlock(&stats_lock);
        queue_enqueue(&vip_queue, promoted[i]);
    }
}

static Order *dequeue_next_order(void) {
    if (scheduling_policy == POLICY_MLQ) {
        maybe_promote_aged_orders_for_mlq();
        Order *order = queue_dequeue_best(&vip_queue, POLICY_FCFS);
        if (order) {
            return order;
        }
        return queue_dequeue_best(&normal_queue, POLICY_FCFS);
    }

    return queue_dequeue_best(&order_queue, scheduling_policy);
}

static void wake_all_threads(void) {
    pthread_mutex_lock(&order_queue.lock);
    pthread_cond_broadcast(&order_queue.not_empty);
    pthread_cond_broadcast(&order_queue.not_full);
    pthread_mutex_unlock(&order_queue.lock);

    pthread_mutex_lock(&vip_queue.lock);
    pthread_cond_broadcast(&vip_queue.not_empty);
    pthread_cond_broadcast(&vip_queue.not_full);
    pthread_mutex_unlock(&vip_queue.lock);

    pthread_mutex_lock(&normal_queue.lock);
    pthread_cond_broadcast(&normal_queue.not_empty);
    pthread_cond_broadcast(&normal_queue.not_full);
    pthread_mutex_unlock(&normal_queue.lock);

    pthread_mutex_lock(&pending_lock);
    pthread_cond_broadcast(&pending_available);
    pthread_mutex_unlock(&pending_lock);

    pthread_mutex_lock(&kitchen_pause_lock);
    pthread_cond_broadcast(&kitchen_resume_cond);
    pthread_mutex_unlock(&kitchen_pause_lock);

    for (int i = 0; i < NUM_CHEFS; i++) {
        sem_post(&kitchen_stations);
    }
}

static void maybe_finalize_shutdown(void) {
    int pending_left = pending_remaining();
    int queues_left = all_queue_sizes();

    if (orders_closed && pending_left == 0 && queues_left == 0) {
        running = 0;
        wake_all_threads();
    }
}

static void print_status(void) {
    int queue_count = all_queue_sizes();
    long completed = 0;
    long queue_wait = 0;
    long turnaround = 0;
    long prep = 0;
    long stars = 0;
    int active = 0;
    time_t elapsed = 0;

    pthread_mutex_lock(&stats_lock);
    completed = completed_orders;
    queue_wait = total_wait_time;
    turnaround = total_turnaround_time;
    prep = total_prep_time;
    stars = starvation_count;
    active = active_chefs;
    pthread_mutex_unlock(&stats_lock);

    elapsed = time(NULL) - simulation_start_time;

    printf("\n%s%s=== Kitchen Status ===%s\n", COLOR_BOLD, COLOR_CYAN, COLOR_RESET);
    printf("Orders in queues: %d\n", queue_count);
    printf("Active chefs: %d\n", active);
    printf("Completed orders: %ld\n", completed);
    printf("Starvation events prevented by aging: %ld\n", stars);

    if (completed > 0) {
        printf("Avg queue wait: %ld sec\n", queue_wait / completed);
        printf("Avg turnaround: %ld sec\n", turnaround / completed);
        printf("Avg prep time: %ld sec\n", prep / completed);
    }
    if (elapsed > 0) {
        double throughput = (double)completed / (double)elapsed;
        double utilization = ((double)prep / ((double)NUM_KITCHEN_STATIONS * (double)elapsed)) * 100.0;
        printf("Throughput: %.2f orders/sec\n", throughput);
        printf("Kitchen utilization equivalent: %.2f%%\n", utilization);
    }
}

static void print_final_statistics(void) {
    long completed = 0;
    long queue_wait = 0;
    long turnaround = 0;
    long prep = 0;
    long stars = 0;
    int remaining = 0;
    time_t elapsed = time(NULL) - simulation_start_time;

    pthread_mutex_lock(&stats_lock);
    completed = completed_orders;
    queue_wait = total_wait_time;
    turnaround = total_turnaround_time;
    prep = total_prep_time;
    stars = starvation_count;
    pthread_mutex_unlock(&stats_lock);

    remaining = all_queue_sizes();

    printf("\n%s%s==========================================%s\n", COLOR_BOLD, COLOR_GREEN, COLOR_RESET);
    printf("FINAL STATISTICS\n");
    printf("==========================================\n");
    printf("Total orders completed: %ld\n", completed);
    printf("Orders still queued: %d\n", remaining);
    printf("Aging-based starvation count: %ld\n", stars);

    if (completed > 0) {
        printf("Average queue waiting time: %ld sec\n", queue_wait / completed);
        printf("Average turnaround time: %ld sec\n", turnaround / completed);
        printf("Average prep time: %ld sec\n", prep / completed);
    }
    if (elapsed > 0) {
        double throughput = (double)completed / (double)elapsed;
        double utilization = ((double)prep / ((double)NUM_KITCHEN_STATIONS * (double)elapsed)) * 100.0;
        printf("Throughput: %.2f orders/sec\n", throughput);
        printf("Kitchen utilization equivalent: %.2f%%\n", utilization);
    }
    printf("==========================================\n%s", COLOR_RESET);
}

static void view_queue(void) {
    printf("\n%s%s=== Current Queue View ===%s\n", COLOR_BOLD, COLOR_CYAN, COLOR_RESET);
    if (scheduling_policy == POLICY_MLQ) {
        print_queue_snapshot("VIP Queue", &vip_queue);
        print_queue_snapshot("Normal Queue", &normal_queue);
    } else {
        print_queue_snapshot("Main Queue", &order_queue);
    }
}

static void handle_sigint(int sig) {
    (void)sig;
    running = 0;
    orders_closed = 1;
    printf(COLOR_BOLD COLOR_MAGENTA "\n[INFO] Shutting down restaurant system...\n" COLOR_RESET);
    wake_all_threads();
}

static void *status_monitor(void *arg) {
    (void)arg;

    while (running) {
        sleep(5);
        if (!running) {
            break;
        }
        print_status();
        pthread_mutex_lock(&stats_lock);
        int active = active_chefs;
        pthread_mutex_unlock(&stats_lock);
        logger_log_queue_status(all_queue_sizes(), active);
    }

    return NULL;
}

static void *waiter_thread(void *arg) {
    int waiter_id = *(int *)arg;
    free(arg);

    while (running) {
        Order new_order;
        int has_order = 0;

        pthread_mutex_lock(&pending_lock);
        while (pending_index >= total_orders && running && !orders_closed) {
            pthread_cond_wait(&pending_available, &pending_lock);
        }

        if (!running) {
            pthread_mutex_unlock(&pending_lock);
            break;
        }

        if (pending_index < total_orders) {
            new_order = pending_orders[pending_index++];
            has_order = 1;
        }
        pthread_mutex_unlock(&pending_lock);

        if (!has_order) {
            if (orders_closed) {
                break;
            }
            continue;
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
        if (new_order.first_enqueue_time == 0) {
            new_order.first_enqueue_time = new_order.enqueue_time;
        }

        enqueue_ready_order(new_order);

        printf(COLOR_GREEN "[WAITER-%d] Added Order #%d for %s\n" COLOR_RESET,
               waiter_id,
               new_order.id,
               new_order.customer_name);
        logger_log_event("Order Enqueued", &new_order);
    }

    maybe_finalize_shutdown();
    printf(COLOR_YELLOW "[WAITER-%d] Stopping...\n" COLOR_RESET, waiter_id);
    return NULL;
}

static void *chef_thread(void *arg) {
    int chef_id = *(int *)arg;
    free(arg);

    while (running || pending_remaining() > 0 || all_queue_sizes() > 0) {
        wait_if_paused();
        sem_wait(&kitchen_stations);

        if (!running && pending_remaining() == 0 && all_queue_sizes() == 0) {
            sem_post(&kitchen_stations);
            break;
        }

        wait_if_paused();
        Order *order = dequeue_next_order();
        if (!order) {
            sem_post(&kitchen_stations);
            if (!running) {
                break;
            }
            continue;
        }

        order->start_time = time(NULL);
        order->chef_id = chef_id;
        order->status = 1;

        long slice = order->remaining_prep_time;
        if (scheduling_policy == POLICY_RR && slice > RR_TIME_SLICE_SEC) {
            slice = RR_TIME_SLICE_SEC;
        }

        pthread_mutex_lock(&stats_lock);
        active_chefs++;
        total_wait_time += (long)(order->start_time - order->enqueue_time);
        pthread_mutex_unlock(&stats_lock);

        printf(COLOR_MAGENTA "[CHEF-%d] Preparing Order #%d | %s | %s | Slice: %ld sec\n" COLOR_RESET,
               chef_id,
               order->id,
               order->customer_name,
               order->items,
               slice);
        logger_log_event("Preparation Started", order);

        sleep((unsigned int)slice);

        order->remaining_prep_time -= (int)slice;
        if (order->remaining_prep_time < 0) {
            order->remaining_prep_time = 0;
        }

        pthread_mutex_lock(&stats_lock);
        total_prep_time += slice;
        pthread_mutex_unlock(&stats_lock);

        if (scheduling_policy == POLICY_RR && order->remaining_prep_time > 0 && running) {
            order->status = 0;
            order->chef_id = -1;
            order->enqueue_time = time(NULL);
            enqueue_ready_order(*order);

            printf(COLOR_YELLOW "[CHEF-%d] Time slice expired for Order #%d, re-queued with %d sec remaining\n" COLOR_RESET,
                   chef_id,
                   order->id,
                   order->remaining_prep_time);

            pthread_mutex_lock(&stats_lock);
            active_chefs--;
            pthread_mutex_unlock(&stats_lock);

            free(order);
            sem_post(&kitchen_stations);
            continue;
        }

        order->completion_time = time(NULL);
        order->status = 2;

        long queue_wait = (long)(order->start_time - order->enqueue_time);
        long turnaround = (long)(order->completion_time - (order->first_enqueue_time ? order->first_enqueue_time : order->enqueue_time));

        pthread_mutex_lock(&stats_lock);
        completed_orders++;
        active_chefs--;
        total_turnaround_time += turnaround;
        pthread_mutex_unlock(&stats_lock);

        printf(COLOR_GREEN "[CHEF-%d] Order #%d completed for %s\n" COLOR_RESET,
               chef_id,
               order->id,
               order->customer_name);
        logger_log_completion(order, chef_id, queue_wait, turnaround);

        free(order);
        sem_post(&kitchen_stations);

        maybe_finalize_shutdown();
    }

    printf(COLOR_YELLOW "[CHEF-%d] Stopping...\n" COLOR_RESET, chef_id);
    return NULL;
}

static void print_control_menu(void) {
    printf("\n%s%s=== Control Menu ===%s\n", COLOR_BOLD, COLOR_CYAN, COLOR_RESET);
    printf("1. Place Order\n");
    printf("2. View Queue\n");
    printf("3. View Statistics\n");
    printf("4. Pause Kitchen\n");
    printf("5. Resume Kitchen\n");
    printf("6. Exit\n");
}

static void run_control_loop(void) {
    int exit_requested = 0;

    while (running && !exit_requested) {
        print_control_menu();
        switch (read_int("Select option: ")) {
            case 1:
                add_order_to_system(1);
                break;
            case 2:
                view_queue();
                break;
            case 3:
                print_status();
                break;
            case 4:
                pause_kitchen();
                break;
            case 5:
                resume_kitchen();
                break;
            case 6:
                orders_closed = 1;
                pthread_cond_broadcast(&pending_available);
                maybe_finalize_shutdown();
                printf(COLOR_YELLOW "[INFO] No new orders will be accepted. Waiting for the kitchen to finish...\n" COLOR_RESET);
                exit_requested = 1;
                break;
            default:
                printf(COLOR_RED "[ERROR] Invalid option.\n" COLOR_RESET);
                break;
        }
    }
}

int main(void) {
    signal(SIGINT, handle_sigint);

    pthread_mutex_init(&stats_lock, NULL);
    pthread_mutex_init(&pending_lock, NULL);
    pthread_cond_init(&pending_available, NULL);
    pthread_mutex_init(&kitchen_pause_lock, NULL);
    pthread_cond_init(&kitchen_resume_cond, NULL);

    queue_init(&order_queue);
    queue_init(&vip_queue);
    queue_init(&normal_queue);

    print_banner();
    print_feature_overview();

    prompt_for_initial_orders();
    scheduling_policy = read_scheduling_policy();

    logger_init("restaurant.log");
    sem_init(&kitchen_stations, 0, NUM_KITCHEN_STATIONS);

    simulation_start_time = time(NULL);

    printf("\nStarting simulation with %d preloaded orders using %s scheduling.\n",
           total_orders,
           scheduling_name(scheduling_policy));

    pthread_t waiter_threads[NUM_WAITERS];
    pthread_t chef_threads[NUM_CHEFS];
    pthread_t status_thread;

    for (int i = 0; i < NUM_WAITERS; i++) {
        int *waiter_id = malloc(sizeof(int));
        *waiter_id = i + 1;
        pthread_create(&waiter_threads[i], NULL, waiter_thread, waiter_id);
    }

    for (int i = 0; i < NUM_CHEFS; i++) {
        int *chef_id = malloc(sizeof(int));
        *chef_id = i + 1;
        pthread_create(&chef_threads[i], NULL, chef_thread, chef_id);
    }

    pthread_create(&status_thread, NULL, status_monitor, NULL);

    /* Main cycle: wait for current batch to finish, then show menu once to add more or exit */
    while (running) {
        /* Wait for current workload to drain */
        while (running && (pending_remaining() > 0 || all_queue_sizes() > 0 || active_chefs > 0)) {
            sleep(1);
        }

        if (!running) break;

        /* Show menu once after batch finishes; allow multiple interactions until user chooses to start a new batch or exit */
        int exit_requested = 0;
        while (running && !exit_requested) {
            print_control_menu();
            int choice = read_int("Select option: ");
            switch (choice) {
                case 1: {
                    int add_count = read_int("How many new orders to add now? ");
                    if (add_count <= 0) {
                        printf(COLOR_YELLOW "No orders added.\n" COLOR_RESET);
                        break;
                    }
                    for (int k = 0; k < add_count; k++) {
                        add_order_to_system(1);
                    }
                    /* break out to let system process new orders */
                    exit_requested = 1;
                    break;
                }
                case 2:
                    view_queue();
                    break;
                case 3:
                    print_status();
                    break;
                case 4:
                    pause_kitchen();
                    break;
                case 5:
                    resume_kitchen();
                    break;
                case 6:
                    printf(COLOR_YELLOW "Exiting by user request. Shutting down after draining queues...\n" COLOR_RESET);
                    orders_closed = 1;
                    running = 0;
                    wake_all_threads();
                    exit_requested = 1;
                    break;
                default:
                    printf(COLOR_RED "[ERROR] Invalid option.\n" COLOR_RESET);
                    break;
            }
        }

        if (!running) break;

        /* loop will wait for newly added orders to finish, or will exit if user chose to quit */
    }

    orders_closed = 1;
    pthread_cond_broadcast(&pending_available);
    maybe_finalize_shutdown();
    wake_all_threads();

    for (int i = 0; i < NUM_WAITERS; i++) {
        pthread_join(waiter_threads[i], NULL);
    }

    for (int i = 0; i < NUM_CHEFS; i++) {
        pthread_join(chef_threads[i], NULL);
    }

    pthread_join(status_thread, NULL);

    print_final_statistics();

    logger_close();
    printf(COLOR_BOLD COLOR_GREEN "System shutdown complete. Check 'restaurant.log' for detailed logs.\n" COLOR_RESET);
    return 0;
}
