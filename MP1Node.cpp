/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for (int i = 0; i < 6; i++) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue <q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    memberNode->bFailed = false;
    memberNode->inGroup = false;
    memberNode->inited = true;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (memcmp((char *) &(memberNode->addr.addr), (char *) &(joinaddr->addr), sizeof(memberNode->addr.addr)) == 0) {
        memberNode->inGroup = true;
    } else {
        size_t messageSize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(messageSize * sizeof(char));

        msg->msgType = JOINREQ;
        memcpy((char *) (msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *) (msg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *) msg, messageSize);

        free(msg);
    }

    return 1;

}

void MP1Node::cleanupNodeState() {
    memberNode->inGroup = false;
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    memberNode->inited = false;
    cleanupNodeState();

    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }
    checkMessages();
    if (!memberNode->inGroup) {
        return;
    }
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *) memberNode, (char *) ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    MessageHdr *receivedMsg = (MessageHdr *) malloc(size * sizeof(char));
    memcpy(receivedMsg, data, sizeof(MessageHdr));

    if (receivedMsg->msgType == JOINREQ) {

        int id;
        short port;
        long heartbeat;
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));
        addToMemberListTable(id, port, heartbeat, memberNode->timeOutCounter);
        Address newNodeAddress = getNodeAddress(id, port);

        sendJOINREP(&newNodeAddress);
    } else if (receivedMsg->msgType == JOINREP) {
        memberNode->inGroup = true;
        deserializeMemberListTableForJOINREP(data);
    } else if (receivedMsg->msgType == HEARTBEAT) {
        int id;
        short port;
        long heartbeat;
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));

        if (!isInMemberListTable(id)) {
            addToMemberListTable(id, port, heartbeat, memberNode->timeOutCounter);
        } else {
            MemberListEntry *node = getFromMemberListTable(id);
            node->setheartbeat(heartbeat);
            node->settimestamp(memberNode->timeOutCounter);
        }
    }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    if (memberNode->pingCounter == 0) {
        memberNode->heartbeat++;

        for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
             it != memberNode->memberList.end(); ++it) {
            Address nodeAddress = getNodeAddress(it->id, it->getport());

            if (!isNodeAddress(&nodeAddress)) {
                sendHEARTBEAT(&nodeAddress);
            }
        }
        memberNode->pingCounter = TFAIL;
    } else {
        memberNode->pingCounter--;
    }

    // Check if any node has failed
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it) {
        Address address = getNodeAddress(it->id, it->getport());

        if (!isNodeAddress(&address)) {
            // If the difference between overall timeOutCounter and the node timestamp
            // is greater than TREMOVE then remove node from membership list
            if (memberNode->timeOutCounter - it->timestamp > TREMOVE) {
                // Remove node from membership list
                memberNode->memberList.erase(it);
                break;
            }
        }
    }
    memberNode->timeOutCounter++;
    return;
}

Address MP1Node::getNodeAddress(int id, short port) {
    Address address;

    memset(&address, 0, sizeof(Address));
    *(int *) (&address.addr) = id;
    *(short *) (&address.addr[4]) = port;

    return address;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *) (&joinaddr.addr) = 1;
    *(short *) (&joinaddr.addr[4]) = 0;

    return joinaddr;
}

bool MP1Node::isNodeAddress(Address *address) {
    return (memcmp((char *) &(memberNode->addr.addr), (char *) &(address->addr), sizeof(memberNode->addr.addr)) == 0);
}

bool MP1Node::isInMemberListTable(int id) {
    return (this->getFromMemberListTable(id) != NULL);
}

MemberListEntry *MP1Node::getFromMemberListTable(int id) {
    MemberListEntry *entry = NULL;

    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it) {
        if (it->id == id) {
            entry = it.base();
            break;
        }
    }

    return entry;
}

void MP1Node::addToMemberListTable(int id, short port, long heartbeat, long timestamp) {
    if (isInMemberListTable(id)) {
        return;
    }
    MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, timestamp);
    memberNode->memberList.insert(memberNode->memberList.end(), *newEntry);
    delete newEntry;
}

void MP1Node::removeNodeFromMemberListTable(int id, short port) {

    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it) {
        if (it->id == id) {
            memberNode->memberList.erase(it);
            break;
        }
    }
}

void MP1Node::sendJOINREQ(Address *joinAddr) {
    size_t messageSize = sizeof(MessageHdr) + sizeof(joinAddr->addr) + sizeof(long) + 1;
    MessageHdr *msg = (MessageHdr *) malloc(messageSize * sizeof(char));

    // Create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *) (msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *) (msg + 1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Trying to join...");
#endif

    // Send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinAddr, (char *) msg, messageSize);

    free(msg);
}

void MP1Node::sendJOINREP(Address *destinationAddr) {
    size_t memberListEntrySize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);

    size_t messageSize = sizeof(MessageHdr) + sizeof(int) + (memberNode->memberList.size() * memberListEntrySize);
    MessageHdr *msg = (MessageHdr *) malloc(messageSize * sizeof(char));
    msg->msgType = JOINREP;

    // Serialize member list
    serializeMemberListTableForJOINREP(msg);

    // Send JOINREP message to the new node
    emulNet->ENsend(&memberNode->addr, destinationAddr, (char *) msg, messageSize);

    free(msg);
}

void MP1Node::sendHEARTBEAT(Address *destinationAddr) {
    size_t messageSize = sizeof(MessageHdr) + sizeof(destinationAddr->addr) + sizeof(long) + 1;
    MessageHdr *msg = (MessageHdr *) malloc(messageSize * sizeof(char));

    // Create HEARTBEAT message
    msg->msgType = HEARTBEAT;
    memcpy((char *) (msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *) (msg + 1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

    // Send HEARTBEAT message to destination node
    emulNet->ENsend(&memberNode->addr, destinationAddr, (char *) msg, messageSize);

    free(msg);
}

void MP1Node::serializeMemberListTableForJOINREP(MessageHdr *msg) {
    // Serialize number of items
    int numItems = memberNode->memberList.size();
    memcpy((char *) (msg + 1), &numItems, sizeof(int));

    // Serialize member list entries
    int offset = sizeof(int);

    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end(); ++it) {
        memcpy((char *) (msg + 1) + offset, &it->id, sizeof(int));
        offset += sizeof(int);

        memcpy((char *) (msg + 1) + offset, &it->port, sizeof(short));
        offset += sizeof(short);

        memcpy((char *) (msg + 1) + offset, &it->heartbeat, sizeof(long));
        offset += sizeof(long);

        memcpy((char *) (msg + 1) + offset, &it->timestamp, sizeof(long));
        offset += sizeof(long);
    }
}

void MP1Node::deserializeMemberListTableForJOINREP(char *data) {
    int numItems;
    memcpy(&numItems, data + sizeof(MessageHdr), sizeof(int));

    // Deserialize member list entries
    int offset = sizeof(int);

    for (int i = 0; i < numItems; i++) {
        int id;
        short port;
        long heartbeat;
        long timestamp;

        memcpy(&id, data + sizeof(MessageHdr) + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(&port, data + sizeof(MessageHdr) + offset, sizeof(short));
        offset += sizeof(short);

        memcpy(&heartbeat, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        memcpy(&timestamp, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        // Create and insert new entry
        addToMemberListTable(id, port, heartbeat, timestamp);
    }
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *) &addr->addr[4]);
}