# COMPSCI 532 - Systems For Data Science Homework Questions
# Question 1
_Due by 26th September 2024_  
Make a table showing the execution times with different i, j, k orders. There are a total of 6 orders.
Explain the results in a report of 100-200 words.
# Question 2
_Due by 7 October 2024_  
Compile and execute the provided code, which measures how long it takes to perform operations on a few different arrays. In the first test, every thread accesses the array in the same position but does not update the value stored in the array.  In the second test, each thread reads and writes to the same position within the array.  In the third test, different threads access distinct elements in the array.  The code outputs the total time for each test as well as the number of threads that that test was run with.  You should update max_threads on line 76 to reflect the number of cores (or hardware threading units if enabled) on your machine. Note that each thread is pinned to a different core.
