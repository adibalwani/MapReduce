# Author: Rachit Puri and Rushikesh Badami

BUCKET_NAME="s3://mapred012"
#OUTPUT="output12"

aws s3 rb $BUCKET_NAME --force
#aws s3 rm s3://$BUCKET_NAME/$OUTPUT --recursive
aws s3 mb $BUCKET_NAME
#aws s3 mb s3://$BUCKET_NAME/$OUTPUT

var=1
while read id
do
	echo $id
	while [ $var != "0" ]; do
			ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "aws s3 cp ./output/* $BUCKET_NAME" &
			var=$?
			echo $var
	done
	var=1
done < original-dns
wait

rm -rf output
mkdir output
aws s3 cp $BUCKET_NAME/ ./output/ --recursive
aws s3 rm $BUCKET_NAME/output --recursive


