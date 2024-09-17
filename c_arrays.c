#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>

// We would be working with square matrices of a definite size
#define N 2

void fillArray(int arr[N][N], int min, int max) {

	// Setting a seed
	unsigned int seed = time(0);

	int i, j;
	for (i = 0; i < N; i++) {
		for (j = 0; j < N; j++) {
			int randomNum = rand_r(&seed) % (max - min + 1) + min;
			arr[i][j] = randomNum;
		}
	}
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
void multiplyIJK(int a[N][N], int b[N][N]) {
	int c[N][N];
	int sum = 0;
	int i, j, k;
	for (i = 0; i < N; i++) {
		sum = 0;
		for (j = 0; j < N; j++) {
			for (k = 0; k < N; k++) {
				// sum += a[i][k] * b[k][j];
				c[i][j] += a[i][k] * b[k][j];
			}
			// c[i][j] = sum;
		}
	}
	printArray(c);
}

void multiplyIKJ(int a[N][N], int b[N][N]) {
	int c[N][N];
	int sum = 0;
	int i, j, k;
	for (i = 0; i < N; i++) {
		sum = 0;
		for (k = 0; k < N; k++) {
			for (j = 0; j < N; j++) {
				// sum += a[i][k] * b[k][j];
				c[i][j] += a[i][k] * b[k][j];
			}
			// c[i][j] = sum;
		}
	}
	// printArray(c);
}

void multiplyJIK(int a[N][N], int b[N][N]) {
	int c[N][N];
	int sum = 0;
	int i, j, k;
	for (j = 0; j < N; j++) {
		sum = 0;
		for (i = 0; i < N; i++) {
			for (k = 0; k < N; k++) {
				// sum += a[i][k] * b[k][j];
				c[i][j] += a[i][k] * b[k][j];
			}
			// c[i][j] = sum;
		}
	}
	printArray(c);
}

void multiplyJKI(int a[N][N], int b[N][N]) {
	int c[N][N];
	int sum = 0;
	int i, j, k;
	for (j = 0; j < N; j++) {
		sum = 0;
		for (k = 0; k < N; k++) {
			for (i = 0; i < N; i++) {
				// sum += a[i][k] * b[k][j];
				c[i][j] += a[i][k] * b[k][j];
			}
			// c[i][j] = sum;
		}
	}
	// printArray(c);
}

void printTimes(struct timeval start, struct timeval end) {
	// Get microseconds
	long ms = end.tv_usec - start.tv_usec;
	printf("\nGTOD = %ld\n", ms);
}

int main() {
	int a[N][N];
	int b[N][N];

	// Fill up the array with random numbers between 1 to 10
	fillArray(a, 1, 5);
	fillArray(b, 1, 5);
	
	// Initialize clock before program start
	struct timeval start, end;
	gettimeofday(&start, NULL);

	// code here
	multiplyIJK(a, b);
	multiplyIKJ(a, b);
	multiplyJIK(a, b);
	multiplyJKI(a, b);

	// Subtract the initial value of t to get time taken
	gettimeofday(&end, NULL);
	printTimes(start, end);
	return 0;
}
