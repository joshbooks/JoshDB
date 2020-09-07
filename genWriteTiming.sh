cat disruptorWriteTimes | sed 's/.* \([0-9]*\) nanoseconds to write \([0-9]*\) pages with \([0-9]*\).*size \([0-9]*\)/\1,\2,\3,\4/' | sort -u 
