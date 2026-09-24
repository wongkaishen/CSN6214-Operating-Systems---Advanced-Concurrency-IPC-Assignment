CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread -D_FILE_OFFSET_BITS=64
LDFLAGS = -lrt -pthread

all: parallel_merge_sort

parallel_merge_sort: main.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

run: parallel_merge_sort
	./parallel_merge_sort

clean:
	rm -f parallel_merge_sort *.o dataset.bin raw_data.csv
