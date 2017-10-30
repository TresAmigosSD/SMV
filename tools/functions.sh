# Bash functions for use in smv

# remove trailing alphanum characters in dot-separated version text
function sanitize_version () {
  # match a digit, followed by a letter, "+" or "_," and anything up to a "."
  # keep just the digit -- essentially removing any trailing alphanum between dots
  echo $(sed -E 's/([0-9])[_+a-zA-Z][^.]*/\1/g' <<< "$1")
}

# compares version-wise the two parameters (a, b) after sanitizing
# using the function above, where a and b are version text that may be
# dot-separated
#
# echoes -1 if a < b,
#         0 if a == b,
#     and 1 if a > b
function ver_cmp () {
  if [[ $1 == $2 ]]; then
    echo 0
    return
  fi
  local a=$(sanitize_version $1) b=$(sanitize_version $2)
  # split $1 by '.' into ver1 and $2 by '.' into ver2
  local IFS=.
  local i ver1=($a) ver2=($b) # same as read -r -a ver1 <<<"$1", etc
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
# and contain only digits, which may be 0-padded
#
# echoes -1 if a < b, 0 if a == b, and 1 if a > b
function ver_cmp0() {
  if (( 10#$1 < 10#$2 )); then
    echo -1
  elif (( 10#$1 > 10#$2 )); then
    echo 1
  else
    echo 0
  fi
}
