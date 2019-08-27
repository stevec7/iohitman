# iohitman

I built this as a pet project because I wanted to mess around with mpi4py and I do a lot of parallel I/O testing. This is not production code, and it was never meant to be. Do as you wish.

# Setup

Build it like you would a normal Python package, although I'm sure you'll get hung up with the mpi4py requirements.

# Usage

Use the run_iohitman.py script thats included in the bin dir. Hopefully I packaged this correctly so that the proper modules are in the PYTHONPATH.

There are 36 different tests that you can run, most of which work, but some don't...need to debug them.

There is other functionality that allows to set the ratio of readers vs writers, write (or read) size ranges, etc. Most of those options work, but I haven't
tested every possible combination.

And as usual, you cannot read from a file that doesnt exist, etc.

See the run_iohitman help docs:

```
Usage: run_iohitman.py [options]

Options:
  -h, --help            show this help message and exit
  -b NUM_BYTES, --buffer-size=NUM_BYTES
                        buffer size in bytes for each rank.
                        DO NOT allocate more than client memory, remember,
                        total amount mem used per node is: ${buffersize} *
                        mpi_ranks_per_node...128MB is a good choice.
  -i LOW_BYTES,HIGH_BYES, --iosz-range=LOW_BYTES,HIGH_BYES
                        if you pass one value, that is the
                        io size. if you pass two, io sizes are generated
                        between that range. Ex: -i 1024 == 1KB, -i 1024,2048
                        == 1KB-2KB. Overwrites -t value
  -k, --keep-files      keep files after run
  -l, --show-test-names
                        Show all valid test names
  -o FILENAME_PREFIX, --output-filename=FILENAME_PREFIX
                        path and filename prefix to write and read to.
                        output file per node will be:
                        ${filename}.${nodename}.dat. Make sure its on a fast
                        filesystem
  -s NUM_BYTES, --block-size=NUM_BYTES
                        number of bytes to write/read per MPI rank.
  -t NUM_BYTES, --transfer-size=NUM_BYTES
                        io transfer size in bytes
  -v, --verbose         enable verbose logging
  --dry-run             show the test info and create the byte range
                        worklists, but dont actually run the tests.
  --ratio=WRITERS_PCT:READERS_PCT
                        If running a test that starts with "mixed",
                        you can set the percentage of writes vs readers.
                        Ignored on other tests
  --read-size=LOW_BYTES,HIGH_BYES
                        read size in bytes. can take multiple args for ranges
                        instead, similar to -i
  --test-name=TESTNAME  REQUIRED. The type of I/O test to run. Pass --show-
                        test-names to see a full list. You can pass the name
                        or number
  --write-size=LOW_BYTES,HIGH_BYES
                        write size in bytes. can take multiple args for ranges
                        instead, similar to -i
  --tags=SOME,TAGS      any custom tags that one would like it see in the
                        output
```

Lastly, don't complain about that testsuites/iotests_runner.py code. I know it's horrible. Well, most of the code is horrible.
