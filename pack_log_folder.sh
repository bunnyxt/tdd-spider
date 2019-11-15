date_str=`date +"%Y-%m-%d"`
file_name=log_pack_${date_str}.tar.gz

echo "Now pack log folder into ${file_name} file..."
tar -czvf ${file_name} log
echo "Finish pack log folder into ${file_name} file!"

rm log/*
echo "Finish clear log folder!"

mv ${file_name} /var/www/res/tdd/log_pack/
echo "File moved to res folder!"
