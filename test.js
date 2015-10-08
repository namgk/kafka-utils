var kafka = require('kafka-node'),
  Consumer = kafka.Consumer,
  client = new kafka.Client('<zookeeper_host>:<zookeeper_port>'),
  offsetClient = new kafka.Offset(client);

var KafkaUtils = require('./kb-kafka-utils'),
	kafkaUtils = KafkaUtils.init('keeboo-location',client);

function test(){
	var topicOffsets = kafkaUtils.buildOffsets(['test_topic1','trip-dev']).then(function(data){
		console.log(JSON.stringify(data));
		process.exit();
	}, function(err){
		console.log(err);
		process.exit();
	})
}

test();