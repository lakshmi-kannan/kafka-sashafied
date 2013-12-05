Kafka HTTP endpoint
-------------------

Rationale
---------
Kafka High level Producer and Consumer clients are very hard to implement right.
HTTP endpoint uses Scala high level consumer and producer client so non-JVM languages can use the features of the high level api.


Formats
--------

Both consumer and producer endpoints accept/return values in json and bson formats. 
Submitted values are stored in bson.


Producer Endpoint API
----------------------

Producer endpoint accepts messages in batches to the topic:

```bash
curl -X POST -H "Content-Type: application/json"\
             -d '{"messages": [{"key": "key", "value":{"val1":"hello"}}]}'\
              http://localhost:8090/topics/topic1
```

Here's the post format explained:

```javascript
{
    // list of messages submitted to kafka
    "messages": [{
        "key": "hello", //kafka message key(optional)
        "value": {"a": "b"} //kafka message value(required). 
     }]
}


```

Endpoint can be configured to be sync or async. This endpoint can be accessed concurrently by multiple clients.


Consumer Endpoint API
----------------------

Endpoint is configured to consume from the particular topic/list of topics upon startup and this setting can not be provided on the request basis.

Consumer endpoint uses long-polling to consume messages in batches in json or bson formats:

Example request:

```bash
curl -H "Accept:application/json" -v http://localhost:8091?batchSize=10
```

Request will block till:

* timeout occurs - in this case the messages consumed during the polling request will be returned (empty if no messages consumed)
* the batch of 10 messages has been consumed.

Example response:

```json
{"messages": [
        {"key": "key", "value": {"a" : "b"}}, 
        {"key": "key1", "value": {"c" : "d"}}
     ]
}
```

Endpoint timeouts, consumer groups and auto commit parameters are configured for every endpoint individually. 
It is also possible to commit offsets explicitly by issuing POST request to the endpoint in case if auto commit has been turned off:

```bash
curl -X POST http://localhost:8091
```

Access to consumer endpoint is serialized, so there should be one one client talking to one consumer endpoint.

