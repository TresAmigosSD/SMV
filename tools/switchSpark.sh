# Import this in ~/.bashrc, you then can switch between spark verion 1.3.0 and
# 1.5.1

s13path="${HOME}/spark-1.3.0-bin-hadoop2.4/bin"
s15path="${HOME}/spark-1.5.1-bin-hadoop2.4/bin"
function useS13() { p_rm $s15path PATH; p_prepend $s13path PATH ;}
function useS15() { p_rm $s13path PATH; p_prepend $s15path PATH ;}

#-----------------------------------------------------------
# Management of PATH, LD_LIBRARY_PATH, MANPATH variables...
# By Emmanuel Rouat <no-email>
# (Inspired by the bash documentation 'pathfuncs' and on
# discussions found on stackoverflow:
# http://stackoverflow.com/questions/370047/
# http://stackoverflow.com/questions/273909/#346860 )
# Last modified: Sat Sep 22 12:01:55 CEST 2012
#
# The following functions handle spaces correctly.
# These functions belong in .bash_profile rather than in
# .bashrc, I guess.
#
# The modular aspect of these functions should make it easy
# to expand them to handle path substitutions instead
# of path removal etc....
#
# See http://www.catonmat.net/blog/awk-one-liners-explained-part-two/
# (item 43) for an explanation of the 'duplicate-entries' removal
# (it's a nice trick!)
#-----------------------------------------------------------

# Show $@ (usually PATH) as list.
function p_show() { local p="$@" && for p; do [[ ${!p} ]] && echo -e ${!p//:/\\n}; done }

# Filter out empty lines, multiple/trailing slashes, and duplicate entries.
function p_filter() { awk '/^[ \t]*$/ {next} {sub(/\/+$/, "");gsub(/\/+/, "/")}!x[$0]++' ;}

# Rebuild list of items into ':' separated word (PATH-like).
function p_build() { paste -sd: - ;}

# Clean $1 (typically PATH) and rebuild it
function p_clean() { local p=${1} && eval ${p}='$(p_show ${p} | p_filter | p_build)' ;}

# Remove $1 from $2 (found on stackoverflow, with modifications).
function p_rm()
{ local d=$(echo $1 | p_filter) p=${2} &&
    eval ${p}='$(p_show ${p} | p_filter | grep -xv "${d}" | p_build)' ;}

#  Same as previous, but filters on a pattern (dangerous...
#+ don't use 'bin' or '/' as pattern!).
function p_rmpat()
{ local d=$(echo $1 | p_filter) p=${2} && eval ${p}='$(p_show ${p} |
    p_filter | grep -v "${d}" | p_build)' ;}

# Delete $1 from $2 and append it cleanly.
function p_append()
{ local d=$(echo $1 | p_filter) p=${2} && p_rm "${d}" ${p} &&
    eval ${p}='$(p_show ${p} d | p_build)' ;}

# Delete $1 from $2 and prepend it cleanly.
function p_prepend()
{ local d=$(echo $1 | p_filter) p=${2} && p_rm "${d}" ${p} &&
    eval ${p}='$(p_show d ${p} | p_build)' ;}


