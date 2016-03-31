BUCKET_NAME="mapred008"
#OUTPUT="output12"

aws s3 rb s3://$BUCKET_NAME --force
#aws s3 rm s3://$BUCKET_NAME/$OUTPUT --recursive
aws s3 mb s3://$BUCKET_NAME
#aws s3 mb s3://$BUCKET_NAME/$OUTPUT

var=1
while read id
do
	echo $id
	while [ $var != "0" ]; do
			ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "aws s3 cp ./output/* s3://mapred008/" &
			var=$?
			echo $var
	done
	var=1
done < original-dns
wait

rm -rf output
mkdir output
aws s3 cp s3://$BUCKET_NAME/ ./output/ --recursive


