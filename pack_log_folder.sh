date_str=`date +"%Y-%m-%d"`
file_name=log_pack_${date_str}.tar.gz

echo "Now move log/*.log.* file into log_tmp folder..."
mkdir log_tmp
mv log/*.log.* log_tmp
echo "Finish move log/*.log.* file into log_tmp folder!"

echo "Now pack log_tmp folder into ${file_name} file..."
tar -czvf ${file_name} log_tmp
echo "Finish pack log folder into ${file_name} file!"

rm -r log_tmp
echo "Finish remove log_tmp folder!"

mv ${file_name} /var/www/res/tdd/log_pack/
echo "File moved to res folder!"
