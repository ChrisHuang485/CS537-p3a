#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <pthread.h>

typedef unsigned int uint;

int MAX_THREAD_NUM;
pthread_mutex_t lock;
int num_thread;

typedef struct
{
  int key;
  char* record;
} key_record;

typedef struct
{
  key_record* key_records;
  int start;
  int end;
  key_record* dup;
} pmergesort_args;


void merge(key_record* key_records, int start, int mid, int end, key_record* dup)
{
    int i = start;
    int j = mid + 1;
    int k = 0;
  
    // Copy data to dup
    while (i <= mid && j <= end) {
      if (key_records[i].key < key_records[j].key) {
        dup[k++] = key_records[i++];
      } else {
        dup[k++] = key_records[j++];
      }
    }
    
    while (i <= mid) {
      dup[k++] = key_records[i++];
    }

    while (j <= end) {
      dup[k++] = key_records[j++];
    }
  
    // Copy back to key_records
    k = 0;
    while (start <= end) {
        key_records[start++] = dup[k++];
    }
}

void* pmergesort(void* args)
{
    pmergesort_args* margs = (pmergesort_args*) args;
    
    key_record* key_records = margs->key_records;
    int start = margs->start;
    int end = margs->end; 
    key_record* dup = margs->dup;

    // check args
    if (start >= end) {
      return NULL;
    }

    // find mid
    int mid = (start + end) >> 1;

    // create 2 new threads
    pthread_t th1;
    pthread_t th2;

    // left part
    pmergesort_args temp_args1 = {key_records, start, mid, dup};
    if (num_thread < MAX_THREAD_NUM) {
      pthread_mutex_lock(&lock);
      num_thread++;
      pthread_mutex_unlock(&lock);

      
      pthread_create(&th1, NULL, pmergesort, &temp_args1);            
      pthread_join(th1, NULL);

      pthread_mutex_lock(&lock);
      num_thread--;
      pthread_mutex_unlock(&lock);
    } else {
      pmergesort(&temp_args1);
    }
    
    // right part
    pmergesort_args temp_args2 = {key_records, mid + 1, end, dup};
    if (num_thread < MAX_THREAD_NUM) {
      pthread_mutex_lock(&lock);
      num_thread++;
      pthread_mutex_unlock(&lock);

      pthread_create(&th2, NULL, pmergesort, &temp_args2);
      pthread_join(th2, NULL);

      pthread_mutex_lock(&lock);
      num_thread--;
      pthread_mutex_unlock(&lock);
    } else {
      pmergesort(&temp_args2);
    }

    merge(key_records, start, mid, end, dup);
    return NULL;
}

int main(int argc, char **argv)
{
  // check num of args
  if (argc < 3)
  {
    fprintf(stderr, "An error has occurred\n");
    exit(0);
  }

  // initialize thread num & its lock
  MAX_THREAD_NUM = get_nprocs();
    
  num_thread = 1;
  pthread_mutex_init(&lock, NULL);

  // define input & output
  struct
  {
    int fd; // file descriptor
    char* map; // mapped file content
    char* fn; // file name
  } input, output;

  input.fn = argv[1];
  output.fn = argv[2];

  // open input & outout files
  if ((input.fd = open(input.fn, O_RDONLY)) == -1 ||
      (output.fd = open(output.fn, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR)) == -1)
  {
    fprintf(stderr, "An error has occurred\n");
    exit(0);
  }

  // get the size of input
  struct stat st;
  stat(input.fn, &st);
  uint input_size = st.st_size;
  // printf("input_size: %d \n", (int)input_size);

  // map to memory (input)
  if ((input.map = mmap(0, input_size, PROT_READ, MAP_SHARED, input.fd, 0)) == MAP_FAILED)
  {
    fprintf(stderr, "An error has occurred\n");
    exit(0);
  }

  // map to memory (output)
  int t_ = ftruncate(output.fd, (int) input_size);
  t_++;
  if ((output.map = mmap(0, input_size, PROT_READ | PROT_WRITE, MAP_SHARED, output.fd, 0)) == MAP_FAILED)
  {
    fprintf(stderr, "An error has occurred\n");
    exit(0);
  }


  uint num_kr = input_size;
  num_kr = num_kr / 100;

  // initialize memory area to store the sorted result
  key_record* kr_map =
      (key_record *)malloc(num_kr * sizeof(key_record));
  key_record* current = kr_map;
  for (char *r = input.map; r < input.map + num_kr * 100; r += 100)
  {
    current->key = *(int *)r;
    current->record = r;
    current++;
  }

  // sort
  // quicksort(kr_map, kr_map + num_kr - 1);
  key_record* dup = (key_record*) malloc(num_kr * sizeof(key_record));
  pmergesort_args temp_args = {kr_map, 0, num_kr - 1, dup};
  pmergesort(&temp_args);
  free(dup);


  // save sorted result
  char* p_out = output.map;
  for (uint i = 0; i < num_kr; i++) {
    memcpy(p_out, kr_map[i].record, 100);
    
    p_out += 100;
  }

  // free & close before exit
  free(kr_map);

  if (munmap(input.map, num_kr) == -1 || munmap(output.map, num_kr) == -1) {
    fprintf(stderr, "An error has occurred\n");
    exit(0);
  }
    
  close(output.fd);
  close(input.fd);

  exit(0);
}