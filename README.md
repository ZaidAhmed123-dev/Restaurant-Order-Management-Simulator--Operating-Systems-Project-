# Restaurant Order Management Simulator

## 📋 Overview

A **multi-threaded Restaurant Order Management Simulator** developed in C that demonstrates core Operating System concepts including **concurrency, synchronization, scheduling, and resource allocation**. This project models a real-world kitchen where waiter threads (producers) generate customer orders and chef threads (consumers) process them concurrently under limited resources.

**Course:** Operating Systems Course Project | **Semester:** Spring 2026 | **Section:** BCS-4H  
**Faculty:** M. Zulfiqar Ali | **Project ID:** 27

---

## 👥 Team Members

| Roll Number | Name | Contribution Areas |
|---|---|---|
| 24K-0500 | Zaid Ahmed | Scheduling & Resource Allocation |
| 24K-0549 | M. Saad Sohail | Threading & Producer-Consumer Core |
| 23K-0899 | Shayan Nemat | Monitoring, Logging & System Management |

---

## 🎯 Key Objectives

1. ✅ Implement a shared order queue with **mutex protection** to prevent race conditions
2. ✅ Simulate multiple waiter (producer) and chef (consumer) threads working **concurrently**
3. ✅ Control access to limited kitchen stations using **POSIX semaphores**
4. ✅ Integrate **priority-based scheduling** (VIP orders processed first)
5. ✅ Provide **real-time monitoring** and **logging** of order processing

---

## 🔧 Operating System Concepts Covered

| Concept | Implementation |
|---|---|
| **Process/Thread Management** | `pthread_create()`, `pthread_join()` for waiter and chef threads |
| **Synchronization** | `pthread_mutex_lock/unlock()` for shared queue protection |
| **Producer-Consumer Pattern** | `pthread_cond_wait()` / `pthread_cond_signal()` for coordination |
| **Concurrency** | Multiple waiters & chefs accessing shared queue simultaneously |
| **Resource Allocation** | Counting semaphores (`sem_init()`, `sem_wait()`, `sem_post()`) for kitchen stations |
| **Priority Scheduling** | VIP orders processed before normal orders |
| **File I/O & Logging** | System calls for logging completed orders to file |
| **Real-Time Monitoring** | Dedicated monitoring thread tracking queue size, wait times, utilization |

---

## 🚀 Getting Started

### Prerequisites

- **OS:** Linux/Ubuntu 22.04 LTS or later
- **Compiler:** GCC (with pthread support)
- **Libraries:** POSIX Threads (pthread), POSIX Semaphores (semaphore)
- **Development Tools:** Make, Git

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/ZaidAhmed123-dev/Restaurant-Order-Management.git
   cd Restaurant-Order-Management
