#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>

// We would be working with square matrices of a definite size
#define N 100

// Defining a typedef to create an array of functions so that we dont work hard to call them
// individually
typedef void (*f)(int[N][N], int[N][N], int[N][N]);

void fillArray(int arr[N][N], int min, int max) {

	// Setting a seed
	for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {

            arr[i][j] = (double) rand() / (double) RAND_MAX;
        }
    }
}

void clearArray(int arr[N][N]) {
	for (int i=0; i<N; i++)
		for (int j=0; j<N; j++)
			arr[i][j] = 0;
}

void printArray(int arr[N][N]) {
	int i, j, k;
	for (i = 0; i < N; i++) {
		for (j = 0; j < N; j++) {
			printf("%d ", arr[i][j]);
		}
		printf("\n");
	}
}


// Multiply in order I, J and K
void multiplyIJK(int a[N][N], int b[N][N], int c[N][N]) {
	int i, j, k;
	for (i = 0; i < N; i++) {
		for (j = 0; j < N; j++) {
			c[i][j] = 0;
			for (k = 0; k < N; k++) {
				c[i][j] += a[i][k] * b[k][j];
			}
		}
	}
}

void multiplyIKJ(int a[N][N], int b[N][N], int c[N][N]) {
	int i, j, k;
	for (i = 0; i < N; i++) {
		for (k = 0; k < N; k++) {
			for (j = 0; j < N; j++) {
				c[i][j] += a[i][k] * b[k][j];
			}
		}
	}
}

void multiplyJIK(int a[N][N], int b[N][N], int c[N][N]) {
	int i, j, k;
	clearArray(c);
	for (j = 0; j < N; j++) {
		for (i = 0; i < N; i++) {
			c[i][j] = 0;
			for (k = 0; k < N; k++) {
				c[i][j] += a[i][k] * b[k][j];
			}
		}
	}
}

void multiplyJKI(int a[N][N], int b[N][N], int c[N][N]) {
	int i, j, k;
	for (j = 0; j < N; j++) {
		for (k = 0; k < N; k++) {
			for (i = 0; i < N; i++) {
				c[i][j] += a[i][k] * b[k][j];
			}
		}
	}
}

void multiplyKIJ(int a[N][N], int b[N][N], int c[N][N]) {
	int i, j, k;
	for (k = 0; k < N; k++) {
		for (i = 0; i < N; i++) {
			for (j = 0; j < N; j++) {
				c[i][j] += a[i][k] * b[k][j];
			}
		}
	}
}

void multiplyKJI(int a[N][N], int b[N][N], int c[N][N]) {
	int i, j, k;
	for (k = 0; k < N; k++) {
		for (j = 0; j < N; j++) {
			for (i = 0; i < N; i++) {
				c[i][j] += a[i][k] * b[k][j];
			}
		}
	}
}

void printTimes(struct timespec start, struct timespec end) {
	
    double time_spent;
    time_spent = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("Elapsed time in seconds: %f \n", time_spent);
}

int main() {
	int a[N][N];
	int b[N][N];
	int c[N][N];

	// Fill up the array with random numbers between 1 to 10
	fillArray(a, 1, 5);
	fillArray(b, 1, 5);

	f multiplix[6] = { *multiplyIJK, *multiplyIKJ, *multiplyJIK, *multiplyJKI, *multiplyKIJ, *multiplyKJI};

	// Initialize clock before program start
	struct timespec start, end;

	// Making spaces for individual runs
	// clock_gettime(CLOCK_REALTIME, &start);
	// multiplyIJK(a, b, c);
	// clock_gettime(CLOCK_REALTIME, &end);
	// printTimes(start, end);

	// clock_gettime(CLOCK_REALTIME, &start);
	// multiplyIKJ(a, b, c);
	// clock_gettime(CLOCK_REALTIME, &end);
	// printTimes(start, end);

	// clock_gettime(CLOCK_REALTIME, &start);
	// multiplyJIK(a, b, c);
	// clock_gettime(CLOCK_REALTIME, &end);
	// printTimes(start, end);

	// clock_gettime(CLOCK_REALTIME, &start);
	// multiplyJKI(a, b, c);
	// clock_gettime(CLOCK_REALTIME, &end);
	// printTimes(start, end);

	// clock_gettime(CLOCK_REALTIME, &start);
	// multiplyKIJ(a, b, c);
	// clock_gettime(CLOCK_REALTIME, &end);
	// printTimes(start, end);

	// clock_gettime(CLOCK_REALTIME, &start);
	// multiplyKJI(a, b, c);
	// clock_gettime(CLOCK_REALTIME, &end);
	// printTimes(start, end);

	
	// Uncomment this block to run all the functions in a loop
	for (int i=0; i<6; i++) {
		clearArray(c);
		clock_gettime(CLOCK_REALTIME, &start);
		multiplix[i](a, b, c);
		clock_gettime(CLOCK_REALTIME, &end);
		printTimes(start, end);
		// printArray(c);
	}
	return 0;
}
