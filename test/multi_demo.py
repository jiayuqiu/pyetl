import time
from multiprocessing.pool import Pool
from concurrent.futures import as_completed, ProcessPoolExecutor

NUMBERS = range(1, 60000)
K = 50

def f(x):
    r = 0
    for k in range(1, K+2):
        r += x ** (1 / k**1.5)
    return ['xx',r]

if __name__ == '__main__':
    if 1:
        print('-------------------\n no multiProcessing:')
        start = time.time()
        l = []
        for nn in NUMBERS:
            result=f(nn)
            l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n multiprocessing.pool.Pool:')
        start = time.time()
        l = []
        pool = Pool(4)
        for num, result in zip(NUMBERS, pool.map(f, NUMBERS)):
            l.append(result)
        pool.close()
        pool.terminate()
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n multiprocessing.pool.Pool, apply_async:')
        start = time.time()
        l = []
        pool = Pool(4)
        res=[]
        for nn in NUMBERS:
            res.append(pool.apply_async(f,(nn,)))
        pool.close()
        print('middle COST: {}'.format(time.time() - start))
        pool.join()
        for rr in res:
            l.append(rr.get())
        pool.terminate()
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n multiprocessing.pool.Pool, apply_async,maxtasksperchild=1000 :')
        start = time.time()
        l = []
        pool = Pool(4,maxtasksperchild=1000)
        res=[]
        for nn in NUMBERS:
            res.append(pool.apply_async(f,(nn,)))
        pool.close()
        print('middle COST: {}'.format(time.time() - start))
        pool.join()
        for rr in res:
            l.append(rr.get())
        pool.terminate()
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor with chunksize,1/4:')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            chunksize, extra = divmod(len(NUMBERS), executor._max_workers * 4)
            print('chunksize',chunksize)
            for num, result in zip(NUMBERS, executor.map(f, NUMBERS, chunksize=chunksize)):
                l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor with chunksize,1/10:')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            chunksize, extra = divmod(len(NUMBERS), executor._max_workers * 10)
            print('chunksize',chunksize)
            for num, result in zip(NUMBERS, executor.map(f, NUMBERS, chunksize=chunksize)):
                l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor with chunksize,1/100:')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            chunksize, extra = divmod(len(NUMBERS), executor._max_workers * 100)
            print('chunksize',chunksize)
            for num, result in zip(NUMBERS, executor.map(f, NUMBERS, chunksize=chunksize)):
                l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor with chunksize,1/300:')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            chunksize, extra = divmod(len(NUMBERS), executor._max_workers * 300)
            print('chunksize',chunksize)
            for num, result in zip(NUMBERS, executor.map(f, NUMBERS, chunksize=chunksize)):
                l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor with chunksize,500:')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            chunksize=500
            print('chunksize',chunksize)
            for num, result in zip(NUMBERS, executor.map(f, NUMBERS, chunksize=chunksize)):
                l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor submit:')
        start = time.time()
        pool_res=[]
        executor=ProcessPoolExecutor(max_workers=4)
        for nn in NUMBERS:
            res=executor.submit(f,nn)
            pool_res.append(res)
        print('middle COST: {}'.format(time.time() - start))
        l = []
        for p_res in as_completed(pool_res):
            result=p_res.result()
            l.append(result)
        executor.shutdown()
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))
    if 1:
        print('-------------------\n ProcessPoolExecutor without chunksize:')
        start = time.time()
        l = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            for num, result in zip(NUMBERS, executor.map(f, NUMBERS)):
                l.append(result)
        print(len(l), l[0])
        print('COST: {}'.format(time.time() - start))

    print('')
