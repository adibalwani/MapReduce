download_lib:
	rm -rf lib
	mkdir lib
	wget -O lib/org.apache.commons.io.jar https://www.dropbox.com/s/g3mujjhpdrkq8ej/org.apache.commons.io.jar?dl=0

compile:
	javac Node.java Reader.java NetworkManager.java Sampling.java

run:
	java Node

clean:
	rm -rf instance-dns instance-ids instances

clean_submission:
	rm -rf ec2-key.pem
