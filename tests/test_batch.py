from mongo_memoize import Memoizer, batch_process


memoizer = Memoizer(host='julia')


def test_func_square(n):
    return n*n


df = batch_process(memoizer, test_func_square, range(30))

print df

