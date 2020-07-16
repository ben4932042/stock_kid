# fib_submit.py
import concurrent.futures


def fib(n):
    if n < 2:
        return 1
    return fib( n - 1 ) + fib( n - 2 )



def A():
    return 'AAAAAAAAAAAAAAA'

with concurrent.futures.ProcessPoolExecutor() as executor:

    s1 = executor.submit( fib, 10 )  # Return future object
    s2 = executor.submit( fib, 20 )
    s3 = executor.submit( fib, 5 )
    s4 = executor.submit( fib, 8 )
    s5 = executor.submit( A,)

print( s1.result() )
print( s2.result() )
print( s3.result() )
print( s4.result() )
print(s5.result())

import concurrent.futures
import stockid

A=stockid.Stockiid.values()
A = list(A)


FIBS = []
FIBS.append(int(A[0]))
FIBS.append(int(A[int((len(A)-1)/2)]))
print(FIBS)
# FIBS = [ 10, 20, 5, 8]
#
#
# def fib(n):
#     if n < 2:
#         return 1
#     return fib( n - 1 ) + fib( n - 2 )
#
#
# def process():
#     with concurrent.futures.ProcessPoolExecutor() as executor:
#         for number, fib_value in zip( FIBS, executor.map( fib, FIBS ) ):
#             print( "%d's fib number is %d" % (number, fib_value) )
#
#
# if __name__ == '__main__':
#     process()


