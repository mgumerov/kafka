# kafka
Async client-server connection via Kafka

To be more specific: REST requests are served via 3.0 servlet model,
which not only allows thread-per-request processing as in modern NIO approach,
but also allows the request handler method to relinquish its worker thread
"until some event happens" instead of blocking-waiting in the thread.

The need to wait comes from the fact that in the proposed model a REST server serves a request
by sending a kafka message to a remote microservice, which after having processed the message
sends back a reply message. So, this conversation happens asynchronously and the REST request
processor has to wait for its completion before it can actually render  received response as 
HTTP response.

In this particular example single process acts both a part of a REST controller and of a "remote" microservice :)

Also there might be some unused junk in the proposed sources, 
because a REST app skeleton was trivially copied here from another project.