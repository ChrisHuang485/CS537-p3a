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

echo "Test Case 1 : Baseline result"
{ time ~cs537-1/tests/p3a/efficiency_tests/benchmark ~cs537-1/tests/p3a/efficiency_tests/eff1.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "Test Case 1 : Your result"
{ time ./psort ~cs537-1/tests/p3a/efficiency_tests/eff1.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "---------------------------------------------------"

echo "Test Case 2 : Baseline result"
{ time ~cs537-1/tests/p3a/efficiency_tests/benchmark ~cs537-1/tests/p3a/efficiency_tests/eff2.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "Test Case 2 : Your result"
{ time ./psort ~cs537-1/tests/p3a/efficiency_tests/eff2.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "---------------------------------------------------"

echo "Test Case 3 : Baseline result"
{ time ~cs537-1/tests/p3a/efficiency_tests/benchmark ~cs537-1/tests/p3a/efficiency_tests/eff3.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "Test Case 3 : Your result"
{ time ./psort ~cs537-1/tests/p3a/efficiency_tests/eff3.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "---------------------------------------------------"

echo "Test Case 4 : Baseline result"
{ time ~cs537-1/tests/p3a/efficiency_tests/benchmark ~cs537-1/tests/p3a/efficiency_tests/eff4.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "Test Case 4 : Your result"
{ time ./psort ~cs537-1/tests/p3a/efficiency_tests/eff4.in ./eff.out; } 2>&1 | grep real
rm -f ./eff.out
echo "---------------------------------------------------"

echo "Clean up test folder $TEST_PATH"
cd $PROJ_PATH
rm -rf $TEST_PATH