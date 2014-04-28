Use Hadoop to get top 10 frequently used passwords in the leaked database of CSDN.

### Clean

<code>mvn clean</code>

### Build

<code>mvn package</code>

### Run

Your need to put file csdn.data into HDFS <code>/user/</code>.

<code>/user/output</code> should NOT be created before running.

<code>hadoop jar Top10PswdCSDN-1.0.jar com.hackecho.hadoop.example/Top10PswdCSDN /user/csdn.data /user/output</code>

### Check the result

<code>hadoop fs -cat /user/output/part-r-00000 | head -10</code>

### The result

	235037	123456789
	212761	12345678
	76348	11111111
	46053	dearbook
	34953	00000000
	20010	123123123
	17794	1234567890
	15033	88888888
	6995	111111111
	5966	147258369

Any questions, contact zlmoment@gmail.com (DO NOT ask me where to download the leak data file ;) )