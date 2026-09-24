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

// Configuration Constants
#define SHM_NAME       "/parallel_sort_shm"
#define SEM_NAME       "/parallel_sort_sem"
#define DATA_FILE      "dataset.bin"
#define CSV_FILE       "raw_data.csv"

// For rapid initial testing, set to a smaller size. Change to 250000000 (1GB) for final run.
#define TOTAL_ELEMENTS 10000000 // 10 million elements (~40MB) for practical testing
#define DATA_SIZE      (TOTAL_ELEMENTS * sizeof(int32_t))
#define CHUNK_IO_SIZE  (4 * 1024 * 1024) // 4 MB chunk size for unbuffered I/O loop

// Structure to define chunk boundaries for workers
typedef struct {
    int start_idx;
    int end_idx;
} Chunk;

// Standard merge function to merge two sorted sub-arrays
static void merge(int32_t *arr, int left, int mid, int right) {
    int n1 = mid - left + 1;
    int n2 = right - mid;

    int32_t *L = malloc(n1 * sizeof(int32_t));
    int32_t *R = malloc(n2 * sizeof(int32_t));

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
}

// Recursive Merge Sort for worker threads/processes
void merge_sort(int32_t *arr, int left, int right) {
    if (left < right) {
        int mid = left + (right - left) / 2;
        merge_sort(arr, left, mid);
        merge_sort(arr, mid + 1, right);
        merge(arr, left, mid, right);
    }
}

// Master N-Way Merge function to combine sorted chunks
void n_way_merge(int32_t *arr, int num_workers, int elements_per_chunk) {
    int32_t *temp_merged = malloc(DATA_SIZE);
    if (!temp_merged) {
        perror("Failed to allocate memory for merge");
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

    for (size_t i = 0; i < TOTAL_ELEMENTS; i++) {
        arr[i] = temp_merged[i];
    }

    free(temp_merged);
    free(chunk_indices);
    free(chunk_ends);
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

        ssize_t written = write(fd, chunk_buffer, bytes_to_write);
        if (written == -1) {
            perror("Write system call failed");
            free(chunk_buffer);
            close(fd);
            exit(EXIT_FAILURE);
        }
        total_bytes_written += written;
    }

    free(chunk_buffer);
    close(fd);
}

// Thread argument structure for pthread implementation
typedef struct {
    int32_t *array;
    int start_idx;
    int end_idx;
} ThreadArgs;

void *thread_sort_routine(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    merge_sort(args->array, args->start_idx, args->end_idx);
    pthread_exit(NULL);
}

int main(void) {
    printf("[Init] Generating dataset via unbuffered system calls (open/write)...\n");
    generate_and_write_dataset();

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
            
            // 1. Setup POSIX Shared Memory
            int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
            ftruncate(shm_fd, DATA_SIZE);
            int32_t *shared_array = mmap(NULL, DATA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

            // 2. Load Data from disk to Shared Memory using unbuffered read() chunk loop
            int file_fd = open(DATA_FILE, O_RDONLY);
            size_t total_read = 0;
            while (total_read < DATA_SIZE) {
                size_t to_read = CHUNK_IO_SIZE;
                if (DATA_SIZE - total_read < CHUNK_IO_SIZE) to_read = DATA_SIZE - total_read;
                ssize_t r = read(file_fd, ((char *)shared_array) + total_read, to_read);
                if (r <= 0) break;
                total_read += r;
            }
            close(file_fd);

            sem_t *sem = sem_open(SEM_NAME, O_CREAT | O_RDWR, 0666, 0);
            int pipe_fds[num_procs][2];
            pid_t children[num_procs];
            int elements_per_chunk = TOTAL_ELEMENTS / num_procs;

            for (int i = 0; i < num_procs; i++) pipe(pipe_fds[i]);

            // Start Clock Timer (excluding file load time)
            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            for (int i = 0; i < num_procs; i++) {
                children[i] = fork();
                if (children[i] == 0) {
                    for (int j = 0; j < num_procs; j++) {
                        close(pipe_fds[j][1]);
                        if (j != i) close(pipe_fds[j][0]);
                    }
                    Chunk c;
                    read(pipe_fds[i][0], &c, sizeof(Chunk));
                    close(pipe_fds[i][0]);

                    merge_sort(shared_array, c.start_idx, c.end_idx);

                    sem_post(sem);
                    sem_close(sem);
                    exit(EXIT_SUCCESS);
                }
            }

            for (int i = 0; i < num_procs; i++) {
                close(pipe_fds[i][0]);
                Chunk c;
                c.start_idx = i * elements_per_chunk;
                c.end_idx = (i == num_procs - 1) ? (TOTAL_ELEMENTS - 1) 
                                                 : ((i + 1) * elements_per_chunk - 1);
                write(pipe_fds[i][1], &c, sizeof(Chunk));
                close(pipe_fds[i][1]);
            }

            for (int i = 0; i < num_procs; i++) sem_wait(sem);
            for (int i = 0; i < num_procs; i++) waitpid(children[i], NULL, 0);

            // Master N-Way Merge Phase
            n_way_merge(shared_array, num_procs, elements_per_chunk);

            clock_gettime(CLOCK_MONOTONIC, &end);
            double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;

            // Correctness Verification
            int ok = verify_sorted(shared_array, TOTAL_ELEMENTS);
            printf("[Process Run %d] Workers: %d | Time: %.4f sec | Status: %s\n", 
                   run, num_procs, elapsed, ok ? "PASS" : "FAIL");

            fprintf(csv, "%d,Process,%d,%.4f\n", run, num_procs, elapsed);

            sem_close(sem);
            sem_unlink(SEM_NAME);
            munmap(shared_array, DATA_SIZE);
            close(shm_fd);
            shm_unlink(SHM_NAME);
        }
    }

    // ==========================================
    // MULTITHREADING EXPERIMENTS (1 to 10 threads)
    // ==========================================
    for (int num_threads = 1; num_threads <= 10; num_threads++) {
        for (int run = 1; run <= 3; run++) {
            
            int32_t *thread_array = malloc(DATA_SIZE);
            int file_fd = open(DATA_FILE, O_RDONLY);
            size_t total_read = 0;
            while (total_read < DATA_SIZE) {
                size_t to_read = CHUNK_IO_SIZE;
                if (DATA_SIZE - total_read < CHUNK_IO_SIZE) to_read = DATA_SIZE - total_read;
                ssize_t r = read(file_fd, ((char *)thread_array) + total_read, to_read);
                if (r <= 0) break;
                total_read += r;
            }
            close(file_fd);

            pthread_t threads[num_threads];
            ThreadArgs t_args[num_threads];
            int elements_per_chunk = TOTAL_ELEMENTS / num_threads;

            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            for (int i = 0; i < num_threads; i++) {
                t_args[i].array = thread_array;
                t_args[i].start_idx = i * elements_per_chunk;
                t_args[i].end_idx = (i == num_threads - 1) ? (TOTAL_ELEMENTS - 1) 
                                                           : ((i + 1) * elements_per_chunk - 1);
                pthread_create(&threads[i], NULL, thread_sort_routine, &t_args[i]);
            }

            for (int i = 0; i < num_threads; i++) {
                pthread_join(threads[i], NULL);
            }

            // Master N-Way Merge Phase for Threads
            n_way_merge(thread_array, num_threads, elements_per_chunk);

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
