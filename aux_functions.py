def count_words(string):
    return len(string.split())


def count_rec(string):
    count = dict()
    string = string.split()
    for words in string:
        if words in count:
            count[words] += 1
        else:
            count[words] = 1

    return count
