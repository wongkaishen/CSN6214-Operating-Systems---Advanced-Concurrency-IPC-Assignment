CC = gcc
CFLAGS = -O2 -Wall -Wextra -D_FILE_OFFSET_BITS=64
LIBS = -pthread -lrt
TARGET = parallel_sort
SRC = parallel_sort.c

all: $(TARGET)

$(TARGET): $(SRC)$(CC) $(CFLAGS)$(SRC) -o $(TARGET)$(LIBS)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET) dataset.bin raw_data.csv
	rm -f /dev/shm/parallel_sort_shm /dev/shm/sem.parallel_sort_sem