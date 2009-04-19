#! /bin/sh

# checking the number of argument. First argument should be the path of rsa key file
if [ $# -ne 1 ]
then
     echo  "args: [path of the rsa key file]"
     exit
fi
 
# if file does not exist then exit
if [ ! -f $1 ]
then
	echo "$1 does not exist"
	exit
fi

     
# getting list of ip addresses 
LIST_OF_IP_ADDRESSESS='ListOfIpAddressess.txt'
if [ -f $LIST_OF_IP_ADDRESSESS ]
then
	rm $LIST_OF_IP_ADDRESSESS
fi

ec2-describe-instances | awk '"INSTANCE"==$1 && "running"==$6 {print $4}' > $LIST_OF_IP_ADDRESSESS
echo  "List of IP Addresses are stored in file ListOfIpAddresses.txt in the current directory"

var=`cat "$LIST_OF_IP_ADDRESSESS"`
for i in $var; do
	ip_address=$i
	echo "Starting memcached server on $ip_address ..."
	`ssh -i $1 root@"$ip_address" 'killall memcached'`
	`scp -i $1 dist/memcached root@"$ip_address":`
	`scp -i $1 dist/libevent-1.4.so.2 root@"$ip_address":`
	`scp -i $1 dist/start_memcached.sh root@"$ip_address":`
	`ssh -i $1 root@"$ip_address" 'chmod +x start_memcached.sh'`
	`ssh -i $1 root@"$ip_address" './start_memcached.sh'`
	echo " ... Success!"
done
