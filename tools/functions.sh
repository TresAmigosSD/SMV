# Bash functions for use in smv

# compares version-wise the two parameters (a, b)
# where a and b are version text that may be dot-separated
#
# echoes -1 if a < b,
#         0 if a == b,
#     and 1 if a > b
function ver_cmp () {
  if [[ $1 == $2 ]]; then
    echo 0
    return
  fi
  # split $1 by '.' into ver1 and $2 by '.' into ver2
  local IFS=.
  local i ver1=($1) ver2=($2) # same as read -r -a ver1 <<<"$1", etc
  # fill empty fields in ver1 with zeros
  for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
    ver1[i]=0
  done
  for ((i=0; i<${#ver1[@]}; i++)); do
    if [[ -z ${ver2[i]} ]]; then
      # fill empty fields in ver2 with zeros
      ver2[i]=0
    fi
    local r=$(ver_cmp0 ${ver1[i]} ${ver2[i]})
    if (( $r != 0 )); then
      echo $r
      return
    fi
  done
  echo 0
  return
}

# compares version-wise two the parameters (a,b)
# where a and b are single version fields, i.e. without dots
# but may contain alphanumeric letters
#
# echoes -1 if a < b, 0 if a == b, and 1 if a > b
function ver_cmp0() {
  local a=$1 b=$2 a2=0 b2=0
  local r1="([0-9]+)[-+a-zA-Z]+$"    # of the form 2arb_text+stuff
  local r2="([0-9]+)[-+a-zA-Z]+([0-9]+)$" # of the form 2rc1

  if [[ $a =~ $r1 ]]; then
    a=${BASH_REMATCH[1]}
  fi
  if [[ $a =~ $r2 ]]; then
    a=${BASH_REMATCH[1]}
    a2=${BASH_REMATCH[2]}
  fi

  if [[ $b =~ $r1 ]]; then
    b=${BASH_REMATCH[1]}
  fi
  if [[ $b =~ $r2 ]]; then
    b=${BASH_REMATCH[1]}
    b2=${BASH_REMATCH[2]}
  fi

  if (( 10#$a < 10#$b )); then
    echo -1
  elif (( 10#$a > 10#$b )); then
    echo 1
  else
    if (( a2 == 0 && b2 == 0 )); then
      echo 0
    elif (( a2 == 0 && b2 != 0)); then
      echo 1  # e.g. 1.2 and 1.2+hot should come after 1.2rc1
    elif (( a2 != 0 && b2 == 0 )); then
      echo -1
    else
      if (( 10#$a2 < 10#$b2 )); then
        echo -1
      elif (( 10#$a2 > 10#$b2 )); then
        echo 1
      else
        echo 0
      fi
    fi
  fi
}
