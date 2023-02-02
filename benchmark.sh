if [ ! -f Makefile ]
then
  echo "Makefile not found."
  exit 1
fi

if [ ! -f psort.c ]
then
  echo "psort.c not found."
  exit 1
fi

PROJ_PATH=`pwd`
echo "Project directory $PROJ_PATH"

TEST_PATH="/dev/shm/${USER}_psort"
echo "Setup test folder $TEST_PATH"

if [ -d $TEST_PATH ]
then
  echo "Old test folder found, removing..."
  rm -rf $TEST_PATH
fi

mkdir $TEST_PATH
cp Makefile psort.c $TEST_PATH
cd $TEST_PATH
echo "Building project..."
make
echo "Warm up Test, this may take seconds or minutes"
time ~cs537-1/tests/p3a/test-psort.sh -c
rm -rf tests-out
echo "Repeat Test 1/3"
time ~cs537-1/tests/p3a/test-psort.sh -c
rm -rf tests-out
echo "Repeat Test 2/3"
time ~cs537-1/tests/p3a/test-psort.sh -c
rm -rf tests-out
echo "Repeat Test 3/3"
time ~cs537-1/tests/p3a/test-psort.sh -c
rm -rf tests-out
echo "Clean up test folder $TEST_PATH"
cd $PROJ_PATH
rm -rf $TEST_PATH