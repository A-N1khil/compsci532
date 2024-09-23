all: ijk ikj jik jki kij kji

## Add file names to all
ijk: ijk.c
	gcc ijk.c -o ijk

ikj: ikj.c
	gcc ikj.c -o ikj

jik: jik.c
	gcc jik.c -o jik

jki: jki.c
	gcc jki.c -o jki

kij: kij.c
	gcc kij.c -o kij

kji: kji.c
	gcc kji.c -o kji

