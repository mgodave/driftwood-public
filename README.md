This is a project I have worked on privately for the last few years. Originally it supported grpc and rsocket but lately the rsocket reactive rpc bindings have fallen into disrepair and there is very little appetite for their use so I dropped support. In general this project is just a lot of unfinished ideas; an excuse for me to use DistributedLog once again, and try some silly ideas out in an interesting space. Lots of stuff doesn't work or isn't tested but if you would like to chat about some of these ideas lmk, I'd be happy to share some of my misadventures.

=========================================================

Driftwood is a read and write proxy for Apache DistributedLog. Currently the only supported library (I didn't look hard) for interacting with DistributedLog is Java based. This tool allows one to expose the DistributedLog library to clients written in other languages by exposing it as a gRPC or RSocket endpoint. The read and write sides have been spit out into separate services but if you wish you could use this as a library and host both in the same process.

`./gradlew distZip` will create a binary package named `streamName-$VERSION.zip` under `driftwood-app/build/distributions`

The basic usage is:

`streamName [readproxy, writeproxy] grpc`

I tried to include all available options for both gRPC and RSocket clients/servers, `GrpcClientOptions.java` and `GrpcServerOptions.java` are the most comprehensive.

This project was/is a learning experience for me using a technology I am familiar with (along with many I am not), it is undocumented, untested, and filled with moments of "WTF was he doing" still I bet there's something here to build upon so if you're interested I'd be happy to talk about it and accept patches.

I added a few tests...
