test_patterns = {
    0: 'segmented',
    1: 'interleaved',
    2: 'random',
}

test_names = {
    0: 'filepernode',
    1: 'shared',
    2: 'fileperprocess',
}

test_types = {
    0: 'write',
    1: 'writeread',
    2: 'read',
    3: 'mixed',
}

# also allow mixed tests to specify per read and per write patterns (segmented, interleaved, random)
custom_tests = ['mixed_filepernode_mixed', 'mixed_shared_mixed']


valid_tests_list = ["{}_{}_{}".format(tt, tn, pa) for pa in test_patterns.values() for tn in test_names.values() for tt in test_types.values()]
valid_tests_list += custom_tests
valid_tests = {e: x for e, x in enumerate(valid_tests_list)}
valid_tests_r = {v: k for k, v in valid_tests.iteritems()}

def print_valid_tests():
    for k, v in valid_tests.iteritems():
        print "{}: {}".format(k, v)
    return
