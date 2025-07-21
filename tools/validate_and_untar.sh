#!/bin/bash


for file in artist release-group ; do
  echo "Processing $file .."
  if [ ! -e $file ] ; then
    echo -n "comparing checksums .."
#    tar xf "${file}.tar.xz" mbdump/
    local_sum=$(md5sum "$file".tar.xz | cut -d ' ' -f 1)
    remote_sum=$(grep "$file".tar.xz MD5SUMS | cut -d ' ' -f 1)

    if [ "$local_sum" == "$remote_sum" ] ; then
      echo "ok"
      echo -n "untarring "
      # GNU tar: tar -xJf "${file}.tar.xz" "mbdump/${file}" --strip-components=1 --checkpoint=10000 --checkpoint-action=dot
      tar -xJOf "$file".tar.xz mbdump/"$file" > "$file" && echo 'ok' || echo 'failed'
    else
      echo "sums didn't match! $local_sum, $remote_sum"
    fi

  else
    echo "file already exists - $file"
    ls -l "${file}"*
  fi

done

