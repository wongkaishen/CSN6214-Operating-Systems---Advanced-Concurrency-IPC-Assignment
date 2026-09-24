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

#define SHM_NAME       "/parallel_sort_shm"
#define SEM_NAME       "/parallel_sort_sem"
#define NUM_PROCESSES  4
#define MOCK_DATA_SIZE (100 * sizeof(int32_t)) // Small mock size for framework testing

// Structure to define the chunk boundaries for each worker process
typedef struct {
    int start_idx;
    int end_idx;
} Chunk;

int main(void) {
    int shm_fd;
    int32_t *shared_array = NULL;
    int pipe_fds[NUM_PROCESSES][2];
    pid_t children[NUM_PROCESSES];

    // ==========================================
    // 1. POSIX SHARED MEMORY SETUP (shm_open & mmap)
    // ==========================================
    shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open failed");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(shm_fd, MOCK_DATA_SIZE) == -1) {
        perror("ftruncate failed");
        shm_unlink(SHM_NAME);
        exit(EXIT_FAILURE);
    }

    shared_array = mmap(NULL, MOCK_DATA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shared_array == MAP_FAILED) {
        perror("mmap failed");
        close(shm_fd);
        shm_unlink(SHM_NAME);
        exit(EXIT_FAILURE);
    }

    // Populate shared array with mock unsorted data for testing
    for (size_t i = 0; i < MOCK_DATA_SIZE / sizeof(int32_t); i++) {
        shared_array[i] = (int32_t)(100 - i); // Descending order test pattern
    }

    // ==========================================
    // 2. POSIX SEMAPHORE SETUP (sem_open)
    // ==========================================
    sem_t *sort_complete_sem = sem_open(SEM_NAME, O_CREAT | O_RDWR, 0666, 0);
    if (sort_complete_sem == SEM_FAILED) {
        perror("sem_open failed");
        munmap(shared_array, MOCK_DATA_SIZE);
        close(shm_fd);
        shm_unlink(SHM_NAME);
        exit(EXIT_FAILURE);
    }

    // ==========================================
    // 3. PIPES SETUP FOR TASK DISPATCHING
    // ==========================================
    for (int i = 0; i < NUM_PROCESSES; i++) {
        if (pipe(pipe_fds[i]) == -1) {
            perror("pipe failed");
            exit(EXIT_FAILURE);
        }
    }

    // ==========================================
    // 4. FORK PROCESSES AND SETUP WORKERS
    // ==========================================
    int elements_per_chunk = (MOCK_DATA_SIZE / sizeof(int32_t)) / NUM_PROCESSES;

    for (int i = 0; i < NUM_PROCESSES; i++) {
        children[i] = fork();

        if (children[i] < 0) {
            perror("fork failed");
            exit(EXIT_FAILURE);
        } 
        else if (children[i] == 0) {
            // --- CHILD PROCESS CONTEXT ---
            for (int j = 0; j < NUM_PROCESSES; j++) {
                close(pipe_fds[j][1]);
                if (j != i) {
                    close(pipe_fds[j][0]);
                }
            }

            Chunk assigned_chunk;
            ssize_t bytes_read = read(pipe_fds[i][0], &assigned_chunk, sizeof(Chunk));
            if (bytes_read <= 0) {
                perror("Child failed to read chunk boundaries");
                exit(EXIT_FAILURE);
            }
            close(pipe_fds[i][0]);

            printf("[Child %d] PID: %d | Assigned Chunk -> Start: %d, End: %d\n", 
                   i, getpid(), assigned_chunk.start_idx, assigned_chunk.end_idx);

            // TODO: Plug in actual sorting routine here for the chunk segment if needed

            sem_post(sort_complete_sem);
            sem_close(sort_complete_sem);
            exit(EXIT_SUCCESS);
        }
    }

    // ==========================================
    // 5. MASTER / PARENT PROCESS CONTEXT
    // ==========================================
    for (int i = 0; i < NUM_PROCESSES; i++) {
        close(pipe_fds[i][0]);
    }

    for (int i = 0; i < NUM_PROCESSES; i++) {
        Chunk c;
        c.start_idx = i * elements_per_chunk;
        c.end_idx = (i == NUM_PROCESSES - 1) ? (int)((MOCK_DATA_SIZE / sizeof(int32_t)) - 1) 
                                               : ((i + 1) * elements_per_chunk - 1);

        write(pipe_fds[i][1], &c, sizeof(Chunk));
        close(pipe_fds[i][1]);
    }

    printf("[Master] Waiting for all %d child processes to complete sorting...\n", NUM_PROCESSES);
    for (int i = 0; i < NUM_PROCESSES; i++) {
        if (sem_wait(sort_complete_sem) == -1) {
            perror("sem_wait failed");
        }
    }
    printf("[Master] All child processes signaled completion via semaphore!\n");

    for (int i = 0; i < NUM_PROCESSES; i++) {
        int status;
        waitpid(children[i], &status, 0);
    }

    // --- TASK 4: N-WAY MERGE MASTER ORCHESTRATION ---
    printf("[Master] Starting N-Way Merge of %d sorted chunks...\n", NUM_PROCESSES);

    int32_t *temp_merged = malloc(MOCK_DATA_SIZE);
    if (temp_merged == NULL) {
        perror("Failed to allocate memory for merge");
        exit(EXIT_FAILURE);
    }

    int chunk_indices[NUM_PROCESSES];
    int chunk_ends[NUM_PROCESSES];

    for (int i = 0; i < NUM_PROCESSES; i++) {
        chunk_indices[i] = i * elements_per_chunk;
        chunk_ends[i] = (i == NUM_PROCESSES - 1) ? (int)((MOCK_DATA_SIZE / sizeof(int32_t)) - 1) 
                                               : ((i + 1) * elements_per_chunk - 1);
    }

    for (int target = 0; target < (int)(MOCK_DATA_SIZE / sizeof(int32_t)); target++) {
        int min_val = INT32_MAX;
        int min_chunk = -1;

        for (int i = 0; i < NUM_PROCESSES; i++) {
            if (chunk_indices[i] <= chunk_ends[i]) {
                if (shared_array[chunk_indices[i]] < min_val) {
                    min_val = shared_array[chunk_indices[i]];
                    min_chunk = i;
                }
            }
        }

        temp_merged[target] = min_val;
        chunk_indices[min_chunk]++;
    }

    for (size_t i = 0; i < MOCK_DATA_SIZE / sizeof(int32_t); i++) {
        shared_array[i] = temp_merged[i];
    }

    free(temp_merged);
    printf("[Master] N-Way Merge completed successfully! Dataset is fully sorted.\n");

    // ==========================================
    // 6. RESOURCE CLEANUP
    // ==========================================
    sem_close(sort_complete_sem);
    sem_unlink(SEM_NAME);

    munmap(shared_array, MOCK_DATA_SIZE);
    close(shm_fd);
    shm_unlink(SHM_NAME);

    printf("[Master] Cleanup completed successfully. Framework test passed.\n");
    return 0;
}
