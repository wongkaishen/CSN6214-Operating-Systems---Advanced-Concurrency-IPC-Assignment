#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

// Configuration Constants
#define SHM_NAME       "/parallel_sort_shm"
#define SEM_NAME       "/parallel_sort_sem"
#define DATA_FILE      "dataset.bin"
#define CSV_FILE       "raw_data.csv"

// 250,000,000 32-bit integers = 1 GB dataset
#define TOTAL_ELEMENTS 250000000 
#define DATA_SIZE      (TOTAL_ELEMENTS * sizeof(int32_t))
#define CHUNK_IO_SIZE  (4 * 1024 * 1024) // 4 MB chunk size for unbuffered I/O

typedef struct {
    int start_idx;
    int end_idx;
} Chunk;

// ======================================================================
// MEMBER 1: FILE I/O & MULTITHREADING SPECIALIST
// Tasks: Low-level unbuffered system calls (open, read, write, close),
//        chunking loop, pthread management, pthread_barrier sync.
// ======================================================================

pthread_barrier_t sync_barrier;

static int write_all(int fd, const void *buffer, size_t count) {
    const char *cursor = buffer;
    size_t offset = 0;

    while (offset < count) {
        ssize_t written = write(fd, cursor + offset, count - offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (written == 0) return -1;
        offset += (size_t)written;
    }
    return 0;
}

static int read_all(int fd, void *buffer, size_t count) {
    char *cursor = buffer;
    size_t offset = 0;

    while (offset < count) {
        ssize_t received = read(fd, cursor + offset, count - offset);
        if (received < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (received == 0) return 0;
        offset += (size_t)received;
    }
    return 1;
}

void generate_and_write_dataset(void) {
    int fd = open(DATA_FILE, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd == -1) {
        perror("Failed to open file for writing");
        exit(EXIT_FAILURE);
    }

    int32_t *chunk_buffer = malloc(CHUNK_IO_SIZE);
    if (!chunk_buffer) {
        perror("Memory allocation failed for generation buffer");
        close(fd);
        exit(EXIT_FAILURE);
    }

    size_t total_bytes_written = 0;
    srand((unsigned int)time(NULL));

    while (total_bytes_written < DATA_SIZE) {
        size_t bytes_to_write = CHUNK_IO_SIZE;
        if (DATA_SIZE - total_bytes_written < CHUNK_IO_SIZE) {
            bytes_to_write = DATA_SIZE - total_bytes_written;
        }

        for (size_t i = 0; i < bytes_to_write / sizeof(int32_t); i++) {
            chunk_buffer[i] = (int32_t)(rand() % 1000000);
        }

        if (write_all(fd, chunk_buffer, bytes_to_write) == -1) {
            perror("Write system call failed");
            free(chunk_buffer);
            close(fd);
            exit(EXIT_FAILURE);
        }
        total_bytes_written += bytes_to_write;
    }

    free(chunk_buffer);
    close(fd);
}

typedef struct {
    int32_t *array;
    int start_idx;
    int end_idx;
} ThreadArgs;

// Forward declaration of Member 4's sorting function
void merge_sort(int32_t *arr, int left, int right);

void *thread_sort_routine(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    merge_sort(args->array, args->start_idx, args->end_idx);
    
    // POSIX Barrier Synchronization for thread phase transition
    pthread_barrier_wait(&sync_barrier);
    pthread_exit(NULL);
}

// ======================================================================
// MEMBER 4: ARCHITECTURE & ALGORITHM SPECIALIST
// Tasks: Core Merge Sort implementation & Master N-Way Merge Algorithm
// ======================================================================

static void merge(int32_t *arr, int left, int mid, int right) {
    int n1 = mid - left + 1;
    int n2 = right - mid;

    int32_t *L = malloc(n1 * sizeof(int32_t));
    int32_t *R = malloc(n2 * sizeof(int32_t));

    for (int i = 0; i < n1; i++) L[i] = arr[left + i];
    for (int j = 0; j < n2; j++) R[j] = arr[mid + 1 + j];

    int i = 0, j = 0, k = left;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) arr[k++] = L[i++];
        else arr[k++] = R[j++];
    }
    while (i < n1) arr[k++] = L[i++];
    while (j < n2) arr[k++] = R[j++];

    free(L);
    free(R);
}

void merge_sort(int32_t *arr, int left, int right) {
    if (left < right) {
        int mid = left + (right - left) / 2;
        merge_sort(arr, left, mid);
        merge_sort(arr, mid + 1, right);
        merge(arr, left, mid, right);
    }
}

void n_way_merge(int32_t *arr, int num_workers, int elements_per_chunk) {
    int32_t *temp_merged = malloc(DATA_SIZE);
    if (!temp_merged) {
        perror("Failed to allocate memory for N-way merge");
        exit(EXIT_FAILURE);
    }

    int *chunk_indices = malloc(num_workers * sizeof(int));
    int *chunk_ends = malloc(num_workers * sizeof(int));

    for (int i = 0; i < num_workers; i++) {
        chunk_indices[i] = i * elements_per_chunk;
        chunk_ends[i] = (i == num_workers - 1) ? (TOTAL_ELEMENTS - 1) 
                                               : ((i + 1) * elements_per_chunk - 1);
    }

    for (int target = 0; target < TOTAL_ELEMENTS; target++) {
        int min_val = INT32_MAX;
        int min_chunk = -1;

        for (int i = 0; i < num_workers; i++) {
            if (chunk_indices[i] <= chunk_ends[i]) {
                if (arr[chunk_indices[i]] < min_val) {
                    min_val = arr[chunk_indices[i]];
                    min_chunk = i;
                }
            }
        }
        temp_merged[target] = min_val;
        chunk_indices[min_chunk]++;
    }

    for (size_t i = 0; i < TOTAL_ELEMENTS; i++) arr[i] = temp_merged[i];

    free(temp_merged);
    free(chunk_indices);
    free(chunk_ends);
}

// ======================================================================
// MEMBER 3: BENCHMARKING & ANALYTICS LEAD
// Tasks: Correctness verification, high-res timing, CSV metrics logging
// ======================================================================

int verify_sorted(int32_t *arr, size_t n) {
    for (size_t i = 0; i < n - 1; i++) {
        if (arr[i] > arr[i + 1]) return 0; // FAIL
    }
    return 1; // PASS
}

static void csv_write(int csv_fd, int run, const char *paradigm,
                      int workers, double elapsed) {
    char line[128];
    int length = snprintf(line, sizeof(line), "%d,%s,%d,%.6f\n",
                          run, paradigm, workers, elapsed);
    if (length < 0 || (size_t)length >= sizeof(line) ||
        write_all(csv_fd, line, (size_t)length) == -1) {
        perror("CSV write failed");
        close(csv_fd);
        exit(EXIT_FAILURE);
    }
}

static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end) {
    return (double)(end->tv_sec - start->tv_sec) +
           (double)(end->tv_nsec - start->tv_nsec) / 1e9;
}

// ======================================================================
// MAIN EXECUTION
// ======================================================================

int main(void) {
    // Member 1: Dataset generation via unbuffered system calls
    printf("[Init] Generating 1GB dataset via unbuffered system calls (open/write)...\n");
    generate_and_write_dataset();

    // Member 3: Benchmark CSV file initialization
    int csv_fd = open(CSV_FILE, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (csv_fd == -1) {
        perror("Failed to create raw_data.csv");
        exit(EXIT_FAILURE);
    }
    const char *csv_header = "Run,Paradigm,Workers,TimeSeconds\n";
    write_all(csv_fd, csv_header, sizeof("Run,Paradigm,Workers,TimeSeconds\n") - 1);

    // ======================================================================
    // MEMBER 2: MULTIPROCESSING EXPERIMENTS
    // Tasks: Shared Memory (shm_open, mmap), Process Creation (fork), 
    //        Pipe IPC, & Named Semaphore Synchronization (sem_open)
    // ======================================================================
    for (int num_procs = 1; num_procs <= 10; num_procs++) {
        for (int run = 1; run <= 3; run++) {
            
            // POSIX Shared Memory setup
            int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
            ftruncate(shm_fd, DATA_SIZE);
            int32_t *shared_array = mmap(NULL, DATA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

            // Member 1 system call logic applied to read dataset directly into SHM
            int file_fd = open(DATA_FILE, O_RDONLY);
            size_t total_read = 0;
            while (total_read < DATA_SIZE) {
                size_t to_read = CHUNK_IO_SIZE;
                if (DATA_SIZE - total_read < CHUNK_IO_SIZE) to_read = DATA_SIZE - total_read;
                read_all(file_fd, ((char *)shared_array) + total_read, to_read);
                total_read += to_read;
            }
            close(file_fd);

            // Semaphore for IPC synchronization
            sem_t *merge_sem = sem_open(SEM_NAME, O_CREAT, 0666, 0);
            
            int pipe_fds[num_procs][2];
            pid_t children[num_procs];
            int elements_per_chunk = TOTAL_ELEMENTS / num_procs;

            for (int i = 0; i < num_procs; i++) {
                if (pipe(pipe_fds[i]) == -1) {
                    perror("Pipe creation failed");
                    exit(EXIT_FAILURE);
                }
            }

            // Member 3: Timing start
            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            // Fork child processes
            for (int i = 0; i < num_procs; i++) {
                children[i] = fork();
                if (children[i] == 0) {
                    for (int j = 0; j < num_procs; j++) {
                        close(pipe_fds[j][1]);
                        if (j != i) close(pipe_fds[j][0]);
                    }
                    Chunk c;
                    read_all(pipe_fds[i][0], &c, sizeof(c));
                    close(pipe_fds[i][0]);

                    // Execute sub-array sorting algorithm (Member 4)
                    merge_sort(shared_array, c.start_idx, c.end_idx);

                    // Signal completion via semaphore
                    sem_post(merge_sem);
                    sem_close(merge_sem);
                    _exit(EXIT_SUCCESS);
                }
            }

            // Master dispatches chunk boundaries to children via pipes
            for (int i = 0; i < num_procs; i++) {
                close(pipe_fds[i][0]);
                Chunk c;
                c.start_idx = i * elements_per_chunk;
                c.end_idx = (i == num_procs - 1) ? (TOTAL_ELEMENTS - 1) 
                                                 : ((i + 1) * elements_per_chunk - 1);
                write_all(pipe_fds[i][1], &c, sizeof(c));
                close(pipe_fds[i][1]);
            }

            // Synchronize with all worker processes using semaphore
            for (int i = 0; i < num_procs; i++) sem_wait(merge_sem);
            for (int i = 0; i < num_procs; i++) waitpid(children[i], NULL, 0);

            // Execute master N-way merge phase (Member 4)
            n_way_merge(shared_array, num_procs, elements_per_chunk);

            // Member 3: Timing stop & verification
            clock_gettime(CLOCK_MONOTONIC, &end);
            double elapsed = elapsed_seconds(&start, &end);
            int ok = verify_sorted(shared_array, TOTAL_ELEMENTS);

            printf("[Process Run %d] Workers: %d | Time: %.4f sec | Status: %s\n", 
                   run, num_procs, elapsed, ok ? "PASS" : "FAIL");
            csv_write(csv_fd, run, "Process", num_procs, elapsed);

            // Cleanup POSIX shared memory and semaphores
            sem_close(merge_sem);
            sem_unlink(SEM_NAME);
            munmap(shared_array, DATA_SIZE);
            close(shm_fd);
            shm_unlink(SHM_NAME);
        }
    }

    // ======================================================================
    // MEMBER 1: MULTITHREADING EXPERIMENTS
    // Tasks: Thread pool dispatch, argument binding, pthread_barrier sync
    // ======================================================================
    for (int num_threads = 1; num_threads <= 10; num_threads++) {
        for (int run = 1; run <= 3; run++) {
            
            int32_t *thread_array = malloc(DATA_SIZE);
            int file_fd = open(DATA_FILE, O_RDONLY);
            size_t total_read = 0;
            while (total_read < DATA_SIZE) {
                size_t to_read = CHUNK_IO_SIZE;
                if (DATA_SIZE - total_read < CHUNK_IO_SIZE) to_read = DATA_SIZE - total_read;
                read_all(file_fd, ((char *)thread_array) + total_read, to_read);
                total_read += to_read;
            }
            close(file_fd);

            // Initialize barrier for worker threads plus main thread
            pthread_barrier_init(&sync_barrier, NULL, num_threads + 1);

            pthread_t threads[num_threads];
            ThreadArgs t_args[num_threads];
            int elements_per_chunk = TOTAL_ELEMENTS / num_threads;

            // Member 3: Timing start
            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            // Dispatch threads
            for (int i = 0; i < num_threads; i++) {
                t_args[i].array = thread_array;
                t_args[i].start_idx = i * elements_per_chunk;
                t_args[i].end_idx = (i == num_threads - 1) ? (TOTAL_ELEMENTS - 1) 
                                                           : ((i + 1) * elements_per_chunk - 1);
                pthread_create(&threads[i], NULL, thread_sort_routine, &t_args[i]);
            }

            // Main thread blocks at barrier until all workers complete chunk sort
            pthread_barrier_wait(&sync_barrier);

            // Master N-Way Merge Phase (Member 4)
            n_way_merge(thread_array, num_threads, elements_per_chunk);

            // Member 3: Timing stop & verification
            clock_gettime(CLOCK_MONOTONIC, &end);
            double elapsed = elapsed_seconds(&start, &end);

            for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);
            pthread_barrier_destroy(&sync_barrier);

            int ok = verify_sorted(thread_array, TOTAL_ELEMENTS);
            printf("[Thread Run %d] Workers: %d | Time: %.4f sec | Status: %s\n", 
                   run, num_threads, elapsed, ok ? "PASS" : "FAIL");
            csv_write(csv_fd, run, "Thread", num_threads, elapsed);

            free(thread_array);
        }
    }

    close(csv_fd);
    printf("[Complete] All experiments completed. Results logged to %s.\n", CSV_FILE);
    return 0;
}