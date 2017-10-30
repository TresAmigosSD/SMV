#!/usr/bin/env bash
set -e
fail_count=0

# test script for version comparison functions
source "${BASH_SOURCE[0]%/*}/functions.sh"

test_vercmp () {
  local r=$(ver_cmp $1 $2) op
  case $r in
    -1) op='<';;
     0) op='=';;
     1) op='>';;
  esac
  if [[ $op != $3 ]]; then
    echo "FAIL: Expected '$3', Actual '$op', Arg1 '$1', Arg2 '$2'"
    (( fail_count += 1 ))
  else
    echo "Pass: '$1 $op $2'"
  fi
}

echo "The following tests should pass"
while read -r test
do
  test_vercmp $test
done << EOF
1            1            =
2.1          2.2          <
3.0.4.10     3.0.4.2      >
4.08         4.08.01      <
3.2.1.9.8144 3.2          >
3.2          3.2.1.9.8144 <
1.2          2.1          <
2.1          1.2          >
5.6.7        5.6.7        =
1.01.1       1.1.1        =
1.1.1        1.01.1       =
1            1.0          =
1.0          1            =
1.0.2.0      1.0.2        =
1..0         1.0          =
1.0          1..0         =
1.5.2rc1     1.5.2        =
1.5.2+hot    1.5.2        =
1.5.2+hot    1.5.2rc2     =
1.5.2rc3     1.5.2rc2     =
1.5.1rc3     1.5.2rc2     <
EOF

echo "The following test should fail"
test_vercmp 1 1 '>'

# set exit code based on expected failed tests
if (( fail_count == 1 )); then
  exit 0
else
  exit $fail_count
fi
