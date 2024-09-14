#include <stdio.h>
#include <time.h>
#include <stdlib.h>

// We would be working with square matrices of a definite size
#define N 3

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

int main() {
	printf("%s - %d\n", "Hello World", N);
	int a[N][N];
	int b[N][N];
	int c[N][N];

	// Fill up the array with random numbers between 1 to 10
	fillArray(a, 1, 10);

	int i, j;
	for (i = 0; i < N; i++) {
		for (j = 0; j< N; j++) {
			printf("%d ", a[i][j]);
		}
		printf("\n");
	}
	return 0;
}
