#!bin/bash
aws s3 ls  s3://cs6240sp16/climate/ > listfiles
echo "$(tail -n +2 listfiles)" > listfiles
i=$(wc -l listfiles|cut -d ' ' -f1)

