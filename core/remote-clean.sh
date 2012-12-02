COMMAND="cd wrench-1.0; find -name *.dat | xargs rm -v; rm db/*.txt"
ssh -i ~/Downloads/hm_us_east.pem ubuntu@ec2-23-20-212-71.compute-1.amazonaws.com "$COMMAND"
ssh -i ~/Downloads/hm_us_east.pem ubuntu@ec2-23-22-168-38.compute-1.amazonaws.com "$COMMAND"

ssh -i ~/Downloads/hm_us_west.pem ubuntu@ec2-54-241-95-241.us-west-1.compute.amazonaws.com "$COMMAND"
ssh -i ~/Downloads/hm_us_west.pem ubuntu@ec2-54-241-95-147.us-west-1.compute.amazonaws.com "$COMMAND"

ssh -i ~/Downloads/hm_us_east.pem ubuntu@ec2-23-22-181-2.compute-1.amazonaws.com "$COMMAND"
ssh -i ~/Downloads/hm_us_east.pem ubuntu@ec2-107-20-94-161.compute-1.amazonaws.com "$COMMAND"
