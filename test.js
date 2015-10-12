var kafka = require('kafka-node'),
  Consumer = kafka.Consumer,
  client = new kafka.Client(process.env.ZOOKEEPER_SERVER),//<zookeeper_host>:<zookeeper_port>
  offsetClient = new kafka.Offset(client);

var KafkaUtils = require('./index.js'),
	kafkaUtils = KafkaUtils.init('test_consumer_group',client);

function test(){
	var topicOffsets = kafkaUtils.buildOffsets(['test_topic','test_topic2']).then(function(data){
		console.log(JSON.stringify(data));
	}, function(err){
		console.log(err);
	})

	var latestOffsets = kafkaUtils.buildLatestOffsets(['test_topic','test_topic2']).then(function(data){
		console.log(JSON.stringify(data));
	}, function(err){
		console.log(err);
	})

	var latestOffsets = kafkaUtils.buildEarliestOffsets(['test_topic','test_topic2']).then(function(data){
		console.log(JSON.stringify(data));
	}, function(err){
		console.log(err);
	})
}

test();