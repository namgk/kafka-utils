var Q = require('q'),
  kafka = require('kafka-node');

function getEarliestAvailOffset(offsetClient, topics){
  var d = Q.defer();

  var wellformedTopics = [];

  topics.forEach(function(topic){
  	var wellformedTopic = {
  		topic: "",
  		partition: 0,
  		time: -2,
  		maxNum: 1
  	};

  	if (typeof topic === 'string'){
  		wellformedTopic.topic = topic;
  		wellformedTopics.push(wellformedTopic);
  		return;
  	} 

  	if (topic.topic)
  		wellformedTopic.topic = topic.topic;
  	else
  		d.reject('topic must have a name');

  	if (topic.partition)
  		wellformedTopic.partition = topic.partition;

  	if (topic.time)
  		wellformedTopic.time = topic.time;

  	if (topic.maxNum)
  		wellformedTopic.maxNum = topic.maxNum;

  	wellformedTopics.push(wellformedTopic);

  })

  offsetClient.fetch(wellformedTopics, function (err, data) {
      if (err)
        d.reject('Error retrieving offsets');
      else
        d.resolve(data);
  });

  return d.promise;
  
}

function getLastCommit(offsetClient, topics, groupId){
  var d = Q.defer();

  var wellformedTopics = [];

  topics.forEach(function(topic){
  	var wellformedTopic = {
  		topic: "",
  		partition: 0
  	}

  	if (typeof topic === 'string'){
  		wellformedTopic.topic = topic;
  		wellformedTopics.push(wellformedTopic);
  		return;
  	}

  	if (topic.topic)
  		wellformedTopic.topic = topic.topic;
  	else
  		d.reject('topic must have a name');

  	if (topic.partition)
  		wellformedTopic.partition = topic.partition;

  	wellformedTopics.push(wellformedTopic);

  })

  offsetClient.fetchCommits(groupId, wellformedTopics, function (err, data) {
      if (err)
        d.reject('Error retrieving last commit');
      else
        d.resolve(data);
    }
  );
  
  return d.promise;
}

function newTopic(earliestAvailOffsetData, lastCommitData, topic){
  if (!earliestAvailOffsetData[topic] && !lastCommitData[topic])
    return;

  var eao = earliestAvailOffsetData[topic]['0'][0];
  var lco = lastCommitData[topic]['0'];
  var offset = (eao > lco) ? eao : lco;
  var topicToBeAdd = { topic: topic, offset: offset };

  return topicToBeAdd;
}

function KafkaUtils(groupId, zookeeper){
  this.groupId = groupId;
  if (typeof zookeeper === 'string')
  	this.client = new kafka.Client(zookeeper);
  else
  	this.client = zookeeper;
  this.offsetClient = new kafka.Offset(this.client)
}

KafkaUtils.prototype.buildOffsets = function(topics){
	var d = Q.defer();

	Q.allSettled([getEarliestAvailOffset(this.offsetClient, topics), getLastCommit(this.offsetClient, topics, this.groupId)])
	.spread(function(earliestAvailOffset, lastCommit){

	  var earliestAvailOffsetData, lastCommitData;

	  if (earliestAvailOffset.state === 'fulfilled'){
	    earliestAvailOffsetData = earliestAvailOffset.value;
	  }

	  if (lastCommit.state === 'fulfilled'){
	    lastCommitData = lastCommit.value;
	  }

	  if (!earliestAvailOffsetData || !lastCommitData)
	    d.reject('cannot get both last commit and earliest available offsets');

	  var topicWithOffsets = [];

	  topics.forEach(function(topic){
	  	topicWithOffsets.push(newTopic(earliestAvailOffsetData, lastCommitData, topic));
	  })

	  d.resolve(topicWithOffsets);

	}, function(){
		d.reject('something wrong')
	});

	return d.promise;
};

module.exports = KafkaUtils;

module.exports.init = function(groupId, zookeeper){
    return new KafkaUtils(groupId, zookeeper);
}
