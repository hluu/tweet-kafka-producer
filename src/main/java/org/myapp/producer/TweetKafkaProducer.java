package org.myapp.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.*;
import java.util.*;



/**
 * Usage: KafkaProducer <base dir> <comma separate sub directories> <optional # of lines>
 */
public class TweetKafkaProducer {

	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.out.println("Usage: TweetKafkaProducer <topic> <base dir> <list of sub directories> <optional # of lines>");
			System.exit(-1);
		}


		String topic = args[0];
		String baseDir = args[1];
		String subDirList = args[2];
		String[] subDirArray = subDirList.split(",");

		int numLines = Integer.MIN_VALUE;
		if (args.length > 3) {
			numLines = Integer.parseInt(args[3]);
		}


		List<File> dirList = buildDirectoryList(baseDir, Arrays.asList(subDirArray));

		System.out.println("========================");
		System.out.println("Topic: " + topic);
		System.out.println("dir list: " + dirList);
		System.out.println("numLines: " + numLines);
		System.out.println("========================");

		int totalCount = 0;
		for (File dir : dirList) {
			System.out.println("Processing directory: " + dir.getAbsolutePath());
			int totalCountPerDir = processFilesInDir(topic, dir, numLines);
			totalCount += totalCountPerDir;
			System.out.printf("directory: %s has %d rows%n", dir.getAbsolutePath(), totalCountPerDir );
		}

		System.out.format("Successfully produced %d records %n", totalCount);
	}

	private static int processFilesInDir(String topic, File dir, int numLines) throws Exception {
		File[] fileList = dir.listFiles();
		Arrays.sort(fileList, new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				return f1.getName().compareTo(f2.getName());
			}
		});

		int totalLines = 0;
		for (File file : fileList) {
			//System.out.println("\t processing file: " + file.getName());
			totalLines += sendTweets(topic, file, numLines);
		}

		return totalLines;
	}

	private static List<File> buildDirectoryList(String basedDirName, List<String> subDirList)
		throws FileNotFoundException {

		File baseDir = new File(basedDirName);

		if (!baseDir.exists()) {
			throw new FileNotFoundException(baseDir.getAbsolutePath() + " doesn't exist");
		}

		List<File> dirList = new LinkedList<File>();

		for (String subDir : subDirList) {
			File dir = new File(baseDir.getAbsolutePath() + File.separator + subDir.trim());
			if (!dir.exists()) {
				throw new FileNotFoundException(dir.getAbsolutePath() + " doesn't exist");
			}
			dirList.add(dir);
		}

		return dirList;

	}

	private static int sendTweets(String topic, File f, int numLine) throws Exception {
		System.out.printf("Sending content of file %s to topic %s%n", f.getAbsolutePath(), topic);
		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("client.id", "Tweets");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


		//System.out.printf("*** Successfully created Kafka producer ***");
		BufferedReader br = new BufferedReader(new FileReader(f));

		String line = null;

		int lineNo = 0;
		while ((line = br.readLine()) != null) {

			//System.out.println(line);
			ProducerRecord record = new ProducerRecord<String,String>(topic,
					line);
			producer.send(record).get();
			lineNo++;

			if (numLine > 0 && lineNo >= numLine) {
				break;
			}
		}

		br.close();
		producer.close();


		return lineNo;
	}
}