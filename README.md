# Message-Queue
Message Queue CLI

Start the message queue server on port 9090, then connect to it using Netcat (nc).


nc 127.0.0.1 9090


Commands
1. Publish a Message
Publish a message to a topic and assign it to a consumer group.

Syntax: mq publish <topic> <message> --group <group_name>
ex: mq publish order order1234 --group payments

2. Consume Messages by Topic
Retrieve messages matching a topic across consumer groups.

Syntax: mq consume <topic>
ex: mq consume order


3. Consume Messages by Topic and Consumer Group
Syntax: mq consume <topic> --group <group_name>
ex: mq consume order --group payments


4. Check Queue Status
Display message statistics for the available consumer groups.
ms status





Field	        Description
queue	        Consumer group name
total_message	Total number of messages in the group
pending	        Number of messages not acknowledged
in_process	    Number of messages classified as in process by the current implementation
ack	            Number of acknowledged messages