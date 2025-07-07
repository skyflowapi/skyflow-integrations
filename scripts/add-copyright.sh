#!/bin/bash
# Copyright (c) 2025 Skyflow, Inc.

# Accepts two arguments:
# 1. The file to add the copyright header to
# 2. A prefix to add to the copyright header
# No spaces are added between the prefix and the copyright header.

tmp_file=$(mktemp)
file="$1"
prefix="$2"
copyright=$prefix"Copyright (c) 2025 Skyflow, Inc."
skip_leading_lines=1

if [ $(head -c 2 "$file") == "#!" ]; then
    # there is a shebang, add the copyright header after the shebang
    shebang=$(head -n 1 "$file")
    # do not add a newline after the copyright header if a shebang was present
    copyright="$shebang\n$copyright"
    skip_leading_lines=2
else
    # no shebang, add a newline after the copyright header
    copyright="$copyright\n"
fi

copyright_len=$(echo -e "$copyright" | wc -c)

head -c $copyright_len "$file" | diff <(echo -e "$copyright") - > /dev/null || ( ( echo -e "$copyright"; tail -n +$skip_leading_lines "$file") > "$tmp_file"; cp "$tmp_file" "$file" )

rm "$tmp_file"
