
csvheader=`head -1 bigfile.csv`
split -d -l10000 bigfile.csv smallfile_
find .|grep smallfile_ | xargs sed -i "1s/^/$csvheader\n/"
sed -i '1d' smallfile_00