# dcb notes


InnerNode should be renamed to just Node, and what is Node now should be Server.

Responsibilities:
    # N

The Raft implementation should be separated as follows:

# `Node`
The `Node` type represents the the Raft replica state machine.

# `Server`
The `Server` type adds listening capability to the Raft Node.
