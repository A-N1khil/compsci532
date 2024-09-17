#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>

// We would be working with square matrices of a definite size
#define N 500

// Defining a typedef to create an array of functions so that we dont work hard to call them
// individually
typedef void (*f)(int[N][N], int[N][N], int[N][N]);

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

void printTimes(struct timeval start, struct timeval end) {
	// Get microseconds
	long ms = end.tv_usec - start.tv_usec;
	printf("\nGTOD = %ld\n", ms);
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
	struct timeval start, end;

	for (int i=0; i<6; i++) {
		clearArray(c);
		gettimeofday(&start, NULL);
		multiplix[i](a, b, c);
		gettimeofday(&end, NULL);
		printTimes(start, end);
		// printArray(c);
	}
	return 0;
}
