all: psort.c
	gcc -g -Wall -Werror -pthread -O -o psort psort.c
	
clean: 
	rm -f psort *.in *.out