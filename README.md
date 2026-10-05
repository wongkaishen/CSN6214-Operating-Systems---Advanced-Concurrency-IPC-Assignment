# Parallel Merge Sort - Multithreading vs Multiprocessing

**CSN6214 Operating Systems**  
**Advanced Concurrency & IPC Assignment**  
**Trimester 2620**

## Description

This project implements a **Parallel Merge Sort** system in C using POSIX APIs.  
It compares the performance of two concurrency models when sorting a large dataset of **250 million 32-bit integers** (~1 GB):

- **Multithreading** using `pthread`
- **Multiprocessing** using `fork()` + POSIX Shared Memory

The program:
1. Generates a 1 GB random dataset using unbuffered system calls (`open` / `write`)
2. Loads the data back using `read()`
3. Sorts the data using 1 to 10 workers (both threads and processes)
4. Performs an N-way merge
5. Verifies the result
6. Records timing data into `raw_data.csv`

## Requirements

- Linux system (tested on Arch Linux)
- GCC compiler
- POSIX threads and real-time libraries

Install required packages (Arch Linux):
```bash
sudo pacman -S gcc

Configuration:
1) Make
2) gcc -O2 -Wall -Wextra -D_FILE_OFFSET_BITS=64 parallel_sort.c -o parallel_sort -pthread -lrt
    ./parallel_sort 


