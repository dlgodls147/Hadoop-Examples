### Clean

<code>mvn clean</code>

### Build

<code>mvn package</code>

### Run

Your need to put file text.txt into HDFS <code>/user/</code>.

<code>/user/output</code> should NOT be created before running.

<code>hadoop jar WordCount-1.0.jar com.hackecho.hadoop.examples.WordCount /user/ /user/output</code>

Any questions, contact zlmoment@gmail.com