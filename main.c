#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <signal.h>

// Configuration Constants
#define SHM_NAME       "/parallel_sort_shm"
#define SEM_NAME       "/parallel_sort_sem"
#define DATA_FILE      "dataset.bin"
#define CSV_FILE       "raw_data.csv"

// For rapid initial testing, set to a smaller size. Change to 250000000 (1GB) for final run.
#define TOTAL_ELEMENTS 10000000 // 10 million elements (~40MB) for practical testing
#define DATA_SIZE      (TOTAL_ELEMENTS * sizeof(int32_t))
#define CHUNK_IO_SIZE  (4 * 1024 * 1024) // 4 MB chunk size for unbuffered I/O loop

// Exclusively create an object we own, then remove its name immediately.
// Open handles/mappings survive unlink and are inherited by forked children.
static int create_shared_memory(void) {
    char name[96];
    for (unsigned attempt = 0; attempt < 128; attempt++) {
        int n = snprintf(name, sizeof(name), "%s_%ld_%u", SHM_NAME, (long)getpid(), attempt);
        if (n < 0 || (size_t)n >= sizeof(name)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        int fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd == -1) {
            if (errno == EEXIST) continue; // Skip stale names without touching their owners.
            return -1;
        }
        if (shm_unlink(name) == -1) {
            int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            return -1;
        }
        return fd;
    }
    errno = EEXIST;
    return -1;
}

static sem_t *create_completion_semaphore(void) {
    char name[96];
    for (unsigned attempt = 0; attempt < 128; attempt++) {
        int n = snprintf(name, sizeof(name), "%s_%ld_%u", SEM_NAME, (long)getpid(), attempt);
        if (n < 0 || (size_t)n >= sizeof(name)) {
            errno = ENAMETOOLONG;
            return SEM_FAILED;
        }
        sem_t *sem = sem_open(name, O_CREAT | O_EXCL, 0600, 0);
        if (sem == SEM_FAILED) {
            if (errno == EEXIST) continue;
            return SEM_FAILED;
        }
        if (sem_unlink(name) == -1) {
            int saved_errno = errno;
            sem_close(sem);
            errno = saved_errno;
            return SEM_FAILED;
        }
        return sem;
    }
    errno = EEXIST;
    return SEM_FAILED;
}

// Structure to define chunk boundaries for workers
typedef struct {
    int start_idx;
    int end_idx;
} Chunk;

// Transfer the complete chunk description; a pipe read may return only part of it.
static int read_chunk(int fd, Chunk *chunk) {
    size_t received = 0;
    while (received < sizeof(*chunk)) {
        ssize_t n = read(fd, (char *)chunk + received, sizeof(*chunk) - received);
        if (n == -1 && errno == EINTR) continue;
        if (n <= 0) {
            if (n == 0) errno = EIO;
            return -1;
        }
        received += (size_t)n;
    }
    return 0;
}

static int write_chunk(int fd, const Chunk *chunk) {
    size_t sent = 0;
    while (sent < sizeof(*chunk)) {
        ssize_t n = write(fd, (const char *)chunk + sent, sizeof(*chunk) - sent);
        if (n == -1 && errno == EINTR) continue;
        if (n <= 0) {
            if (n == 0) errno = EIO;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

// Standard merge function to merge two sorted sub-arrays
static int merge(int32_t *arr, int left, int mid, int right) {
    int n1 = mid - left + 1;
    int n2 = right - mid;

    int32_t *L = malloc(n1 * sizeof(int32_t));
    if (!L) return -1;
    int32_t *R = malloc(n2 * sizeof(int32_t));
    if (!R) {
        int saved_errno = errno;
        free(L);
        errno = saved_errno;
        return -1;
    }

    for (int i = 0; i < n1; i++) L[i] = arr[left + i];
    for (int j = 0; j < n2; j++) R[j] = arr[mid + 1 + j];

    int i = 0, j = 0, k = left;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k++] = L[i++];
        } else {
            arr[k++] = R[j++];
        }
    }
    while (i < n1) arr[k++] = L[i++];
    while (j < n2) arr[k++] = R[j++];

    free(L);
    free(R);
    return 0;
}

// Recursive Merge Sort for worker threads/processes
int merge_sort(int32_t *arr, int left, int right) {
    if (left < right) {
        int mid = left + (right - left) / 2;
        if (merge_sort(arr, left, mid) == -1 ||
            merge_sort(arr, mid + 1, right) == -1) return -1;
        return merge(arr, left, mid, right);
    }
    return 0;
}

// Master N-Way Merge function to combine sorted chunks
int n_way_merge(int32_t *arr, int num_workers, int elements_per_chunk) {
    int32_t *temp_merged = malloc(DATA_SIZE);
    if (!temp_merged) return -1;

    int *chunk_indices = malloc(num_workers * sizeof(int));
    if (!chunk_indices) {
        int saved_errno = errno;
        free(temp_merged);
        errno = saved_errno;
        return -1;
    }
    int *chunk_ends = malloc(num_workers * sizeof(int));
    if (!chunk_ends) {
        int saved_errno = errno;
        free(chunk_indices);
        free(temp_merged);
        errno = saved_errno;
        return -1;
    }

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
                // Select the first available chunk even when its value is INT32_MAX.
                if (min_chunk == -1 || arr[chunk_indices[i]] < min_val) {
                    min_val = arr[chunk_indices[i]];
                    min_chunk = i;
                }
            }
        }
        temp_merged[target] = min_val;
        chunk_indices[min_chunk]++;
    }

    for (size_t i = 0; i < TOTAL_ELEMENTS; i++) {
        arr[i] = temp_merged[i];
    }

    free(temp_merged);
    free(chunk_indices);
    free(chunk_ends);
    return 0;
}

// Correctness Verification: O(N) linear scan check
int verify_sorted(int32_t *arr, size_t n) {
    for (size_t i = 0; i < n - 1; i++) {
        if (arr[i] > arr[i + 1]) {
            return 0; // FAIL
        }
    }
    return 1; // PASS
}

// Function to generate data and write to disk using unbuffered system calls in chunks
void generate_and_write_dataset(void) {
    int fd = open(DATA_FILE, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd == -1) {
        perror("Failed to open file for writing");
        exit(EXIT_FAILURE);
    }

    int32_t *chunk_buffer = malloc(CHUNK_IO_SIZE);
    if (!chunk_buffer) {
        perror("Failed to allocate dataset buffer");
        close(fd);
        exit(EXIT_FAILURE);
    }
    size_t total_bytes_written = 0;

    while (total_bytes_written < DATA_SIZE) {
        size_t bytes_to_write = CHUNK_IO_SIZE;
        if (DATA_SIZE - total_bytes_written < CHUNK_IO_SIZE) {
            bytes_to_write = DATA_SIZE - total_bytes_written;
        }

        for (size_t i = 0; i < bytes_to_write / sizeof(int32_t); i++) {
            chunk_buffer[i] = (int32_t)(rand() % 1000000);
        }

        // Finish this buffer before generating new values, even after a short write.
        size_t chunk_written = 0;
        while (chunk_written < bytes_to_write) {
            ssize_t written = write(fd, (char *)chunk_buffer + chunk_written,
                                    bytes_to_write - chunk_written);
            if (written == -1 && errno == EINTR) continue;
            if (written <= 0) {
                if (written == 0) errno = EIO; // No progress: avoid an infinite loop.
                perror("Write system call failed");
                free(chunk_buffer);
                close(fd);
                exit(EXIT_FAILURE);
            }
            chunk_written += (size_t)written;
        }
        total_bytes_written += bytes_to_write;
    }

    free(chunk_buffer);
    if (close(fd) == -1) {
        perror("Failed to close dataset after writing");
        exit(EXIT_FAILURE);
    }
}

// Return success only after the entire dataset is loaded and the file is closed.
static int load_dataset(int32_t *array) {
    int fd = open(DATA_FILE, O_RDONLY);
    if (fd == -1) return -1;

    size_t total_read = 0;
    while (total_read < DATA_SIZE) {
        size_t to_read = DATA_SIZE - total_read;
        if (to_read > CHUNK_IO_SIZE) to_read = CHUNK_IO_SIZE;

        ssize_t r = read(fd, (char *)array + total_read, to_read);
        if (r == -1 && errno == EINTR) continue;
        if (r <= 0) {
            // EOF before DATA_SIZE is an incomplete dataset, not a successful load.
            int saved_errno = (r == 0) ? EIO : errno;
            close(fd);
            errno = saved_errno;
            return -1;
        }
        total_read += (size_t)r;
    }

    return close(fd);
}

// Thread argument structure for pthread implementation
typedef struct {
    int32_t *array;
    int start_idx;
    int end_idx;
    int sort_status; // Written by the worker and read by the parent after pthread_join.
} ThreadArgs;

void *thread_sort_routine(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    args->sort_status = merge_sort(args->array, args->start_idx, args->end_idx);
    pthread_exit(NULL);
}

int main(void) {
    // A dead pipe reader must produce EPIPE, rather than terminate the parent.
    struct sigaction pipe_action = {0};
    pipe_action.sa_handler = SIG_IGN;
    if (sigemptyset(&pipe_action.sa_mask) == -1 ||
        sigaction(SIGPIPE, &pipe_action, NULL) == -1) {
        perror("Failed to ignore SIGPIPE");
        return EXIT_FAILURE;
    }

    // Seed once so consecutive runs continue the random sequence, even within one second.
    srand((unsigned int)time(NULL));

    // Open CSV file for recording benchmarks
    FILE *csv = fopen(CSV_FILE, "w");
    if (!csv) {
        perror("Failed to create raw_data.csv");
        exit(EXIT_FAILURE);
    }
    fprintf(csv, "Run,Paradigm,Workers,TimeSeconds\n");

    // ==========================================
    // MULTIPROCESSING EXPERIMENTS (1 to 10 processes)
    // ==========================================
    for (int num_procs = 1; num_procs <= 10; num_procs++) {
        for (int run = 1; run <= 3; run++) {
            printf("[Process Run %d] Workers: %d | Generating fresh dataset...\n", run, num_procs);
            generate_and_write_dataset();
            
            // 1. Setup POSIX Shared Memory
            int shm_fd = create_shared_memory();
            if (shm_fd == -1) {
                perror("Failed to create shared memory");
                fclose(csv);
                return EXIT_FAILURE;
            }
            if (ftruncate(shm_fd, DATA_SIZE) == -1) {
                perror("Failed to size shared memory");
                close(shm_fd);
                fclose(csv);
                return EXIT_FAILURE;
            }
            int32_t *shared_array = mmap(NULL, DATA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
            if (shared_array == MAP_FAILED) {
                perror("Failed to map shared memory");
                close(shm_fd);
                fclose(csv);
                return EXIT_FAILURE;
            }

            // 2. Load Data from disk to Shared Memory using unbuffered read() chunk loop
            if (load_dataset(shared_array) == -1) {
                perror("Failed to load complete process dataset");
                munmap(shared_array, DATA_SIZE);
                close(shm_fd);
                fclose(csv);
                return EXIT_FAILURE;
            }

            sem_t *sem = create_completion_semaphore();
            if (sem == SEM_FAILED) {
                perror("Failed to create completion semaphore");
                munmap(shared_array, DATA_SIZE);
                close(shm_fd);
                fclose(csv);
                return EXIT_FAILURE;
            }
            int pipe_fds[num_procs][2];
            pid_t children[num_procs];
            int elements_per_chunk = TOTAL_ELEMENTS / num_procs;

            for (int i = 0; i < num_procs; i++) {
                if (pipe(pipe_fds[i]) == -1) {
                    perror("Failed to create worker pipe");
                    // Only earlier pipes have valid descriptors; no children exist yet.
                    for (int j = 0; j < i; j++) {
                        close(pipe_fds[j][0]);
                        close(pipe_fds[j][1]);
                    }
                    sem_close(sem);
                    munmap(shared_array, DATA_SIZE);
                    close(shm_fd);
                    fclose(csv);
                    return EXIT_FAILURE;
                }
            }

            // Start Clock Timer (excluding file load time)
            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            for (int i = 0; i < num_procs; i++) {
                children[i] = fork();
                if (children[i] == -1) {
                    perror("Failed to fork worker process");
                    // Earlier children may be blocked reading their chunk boundaries.
                    // Terminate only successfully created children, then reap them.
                    for (int j = 0; j < i; j++) {
                        if (kill(children[j], SIGKILL) == -1 && errno != ESRCH) {
                            perror("Failed to terminate worker process");
                        }
                    }
                    for (int j = 0; j < num_procs; j++) {
                        close(pipe_fds[j][0]);
                        close(pipe_fds[j][1]);
                    }
                    for (int j = 0; j < i; j++) {
                        pid_t waited;
                        do {
                            waited = waitpid(children[j], NULL, 0);
                        } while (waited == -1 && errno == EINTR);
                        if (waited == -1) perror("Failed to reap worker process");
                    }
                    sem_close(sem);
                    munmap(shared_array, DATA_SIZE);
                    close(shm_fd);
                    fclose(csv);
                    return EXIT_FAILURE;
                }
                if (children[i] == 0) {
                    for (int j = 0; j < num_procs; j++) {
                        close(pipe_fds[j][1]);
                        if (j != i) close(pipe_fds[j][0]);
                    }
                    Chunk c;
                    if (read_chunk(pipe_fds[i][0], &c) == -1) {
                        perror("Worker failed to receive chunk boundaries");
                        close(pipe_fds[i][0]);
                        sem_close(sem);
                        _exit(EXIT_FAILURE);
                    }
                    if (close(pipe_fds[i][0]) == -1) {
                        perror("Worker failed to close task pipe");
                        sem_close(sem);
                        _exit(EXIT_FAILURE);
                    }

                    if (merge_sort(shared_array, c.start_idx, c.end_idx) == -1) {
                        perror("Worker failed to allocate merge buffers");
                        sem_close(sem);
                        _exit(EXIT_FAILURE);
                    }

                    if (sem_post(sem) == -1) {
                        perror("Worker failed to post completion");
                        sem_close(sem);
                        _exit(EXIT_FAILURE);
                    }
                    if (sem_close(sem) == -1) {
                        perror("Worker failed to close semaphore");
                        _exit(EXIT_FAILURE);
                    }
                    _exit(EXIT_SUCCESS); // Do not flush the parent's inherited CSV buffer.
                }
            }

            int workers_ok = 1;
            for (int i = 0; i < num_procs; i++) {
                if (close(pipe_fds[i][0]) == -1) {
                    perror("Failed to close parent pipe read end");
                    workers_ok = 0;
                }
                Chunk c;
                c.start_idx = i * elements_per_chunk;
                c.end_idx = (i == num_procs - 1) ? (TOTAL_ELEMENTS - 1) 
                                                 : ((i + 1) * elements_per_chunk - 1);
                if (write_chunk(pipe_fds[i][1], &c) == -1) {
                    perror("Failed to send worker chunk boundaries");
                    workers_ok = 0;
                }
                // Close even after failure so an incomplete message ends with EOF.
                if (close(pipe_fds[i][1]) == -1) {
                    perror("Failed to close parent pipe write end");
                    workers_ok = 0;
                }
            }

            // Reap first: a child that crashes cannot provide a semaphore notification.
            for (int i = 0; i < num_procs; i++) {
                int status;
                pid_t waited;
                do {
                    waited = waitpid(children[i], &status, 0);
                } while (waited == -1 && errno == EINTR);
                if (waited == -1) {
                    perror("Failed to wait for worker process");
                    workers_ok = 0;
                } else if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS) {
                    fprintf(stderr, "Worker process %ld did not exit successfully\n",
                            (long)children[i]);
                    workers_ok = 0;
                }
            }

            // All children have exited; missing notifications must fail, never block.
            for (int i = 0; workers_ok && i < num_procs; i++) {
                int rc;
                do {
                    rc = sem_trywait(sem);
                } while (rc == -1 && errno == EINTR);
                if (rc == -1) {
                    perror("Failed to collect worker completion");
                    workers_ok = 0;
                }
            }
            if (!workers_ok) {
                sem_close(sem);
                munmap(shared_array, DATA_SIZE);
                close(shm_fd);
                fclose(csv);
                return EXIT_FAILURE;
            }

            // Master N-Way Merge Phase
            if (n_way_merge(shared_array, num_procs, elements_per_chunk) == -1) {
                perror("Failed to allocate final process merge buffers");
                sem_close(sem);
                munmap(shared_array, DATA_SIZE);
                close(shm_fd);
                fclose(csv);
                return EXIT_FAILURE;
            }

            clock_gettime(CLOCK_MONOTONIC, &end);
            double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;

            // Correctness Verification
            int ok = verify_sorted(shared_array, TOTAL_ELEMENTS);
            printf("[Process Run %d] Workers: %d | Time: %.4f sec | Status: %s\n", 
                   run, num_procs, elapsed, ok ? "PASS" : "FAIL");

            fprintf(csv, "%d,Process,%d,%.4f\n", run, num_procs, elapsed);

            sem_close(sem);
            munmap(shared_array, DATA_SIZE);
            close(shm_fd);
        }
    }

    // ==========================================
    // MULTITHREADING EXPERIMENTS (1 to 10 threads)
    // ==========================================
    for (int num_threads = 1; num_threads <= 10; num_threads++) {
        for (int run = 1; run <= 3; run++) {
            printf("[Thread Run %d] Workers: %d | Generating fresh dataset...\n", run, num_threads);
            generate_and_write_dataset();
            
            int32_t *thread_array = malloc(DATA_SIZE);
            if (!thread_array) {
                perror("Failed to allocate thread dataset");
                fclose(csv);
                return EXIT_FAILURE;
            }
            if (load_dataset(thread_array) == -1) {
                perror("Failed to load complete thread dataset");
                free(thread_array);
                fclose(csv);
                return EXIT_FAILURE;
            }

            pthread_t threads[num_threads];
            ThreadArgs t_args[num_threads];
            int elements_per_chunk = TOTAL_ELEMENTS / num_threads;
            int threads_created = 0;
            int thread_sorts_ok = 1;

            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            for (int i = 0; i < num_threads; i++) {
                t_args[i].array = thread_array;
                t_args[i].start_idx = i * elements_per_chunk;
                t_args[i].end_idx = (i == num_threads - 1) ? (TOTAL_ELEMENTS - 1) 
                                                           : ((i + 1) * elements_per_chunk - 1);
                t_args[i].sort_status = -1;
                int rc = pthread_create(&threads[i], NULL, thread_sort_routine, &t_args[i]);
                if (rc != 0) {
                    errno = rc; // pthread functions return the error code directly.
                    perror("Failed to create worker thread");
                    break;
                }
                threads_created++;
            }

            for (int i = 0; i < threads_created; i++) {
                int rc = pthread_join(threads[i], NULL);
                if (rc != 0) {
                    errno = rc;
                    perror("Failed to join worker thread");
                    fclose(csv);
                    // A worker may still access the array: terminate without freeing it.
                    exit(EXIT_FAILURE);
                }
                if (t_args[i].sort_status == -1) {
                    fprintf(stderr, "Worker thread %d failed to allocate merge buffers\n", i);
                    thread_sorts_ok = 0;
                }
            }

            if (threads_created != num_threads || !thread_sorts_ok) {
                free(thread_array);
                fclose(csv);
                return EXIT_FAILURE;
            }

            // Master N-Way Merge Phase for Threads
            if (n_way_merge(thread_array, num_threads, elements_per_chunk) == -1) {
                perror("Failed to allocate final thread merge buffers");
                free(thread_array);
                fclose(csv);
                return EXIT_FAILURE;
            }

            clock_gettime(CLOCK_MONOTONIC, &end);
            double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;

            int ok = verify_sorted(thread_array, TOTAL_ELEMENTS);
            printf("[Thread Run %d] Workers: %d | Time: %.4f sec | Status: %s\n", 
                   run, num_threads, elapsed, ok ? "PASS" : "FAIL");

            fprintf(csv, "%d,Thread,%d,%.4f\n", run, num_threads, elapsed);

            free(thread_array);
        }
    }

    fclose(csv);
    printf("[Complete] All experiments completed. Results logged to %s.\n", CSV_FILE);
    return 0;
}
