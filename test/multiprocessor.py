
import time

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed


def test(num):
    import time
    return time.ctime(),num


def square(y):
    time.sleep(1)
    return 2 * y


class MacsAdd(object):
    def __init__(self, n):
        self.range_num = range(n)

    def square(self, y):
        time.sleep(1)
        return 2 * y

    def multi_do(self, a=2):
        t1 = time.time()
        res_list = []
        with ProcessPoolExecutor(max_workers=4) as executor:
            # chunksize = 3
            # for result in executor.map(self.square, self.range_num, chunksize=chunksize):
            #     res_list.append(result)

            # for result in executor.map(self.square, self.range_num, ):
            #     res_list.append(result)
            
            for n in self.range_num:
                res_list.append(executor.submit(self.square, n, ))
            
            for res in as_completed(res_list):
                print(res.result())

        t2 = time.time()
        print(f"use time {t2 - t1} seconds.")
        return res_list

    def do(self, a=2):
        t1 = time.time()
        res_list = []
        for i in self.range_num:
            res_list.append(square(i))
        t2 = time.time()
        print(f"use time {t2 - t1} seconds.")
        return res_list


def multi_do(n):
    t1 = time.time()
    res_list = []
    with ProcessPoolExecutor(max_workers=4) as executor:
        chunksize = 3
        for result in executor.map(square, range(n), chunksize=chunksize):
            res_list.append(result)

    t2 = time.time()
    print(f"use time {t2 - t1} seconds.")
    return res_list


if __name__ == '__main__':
    n = 10
    ma = MacsAdd(n)
    print(ma.do(2))
    print(ma.multi_do(2))
    print(multi_do(n))

