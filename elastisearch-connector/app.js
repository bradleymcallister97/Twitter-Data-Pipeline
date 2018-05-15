const elasticsearch = require('elasticsearch');
const amqp = require('amqp');

// docker run -p 9200:9200 -d -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.2.3
var client = new elasticsearch.Client({
    host: process.env.ELASTICSEARCH_HOST,
    log: 'trace'
});

var connection = amqp.createConnection({ 
    url: process.env.CLOUDAMQP_URL
});

connection.on('error', (e) => {
    console.log("Error from amqp: ", e);
});

connection.on('ready', () => {
    connection.queue('', {autoDelete: true}, function(q) {
        // Catch all messages
        console.log(`Queue ${q.name} is open`);
        q.bind('twitter.exchange', 'twitter.word.count');
        
        // Receive messages
        q.subscribe((message) => {
            var data = null;
            try {
                data = JSON.parse(message.data.toString());
            } catch (e) {
                console.log("Error parsing message:", message.data.toString());
            }
            if (data !== null) {
                client.index({
                    index: 'wordcount',
                    type: '_doc',
                    body: data
                }, function (error, response) {
                    if (error) {
                        console.trace('error with elasticsearch:', error);
                    } else {
                        console.log(response);
                    }
                });
            }
        });
    });
});
