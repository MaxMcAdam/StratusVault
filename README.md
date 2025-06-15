## Overview

This is a gRPC-based file system tool. There are 3 main components. The protobuf type definitions as well as the generated gRPC server interface and client are defined in the proto folder. The client cli is in the client folder and the implemented server is in the server folder.

# Client 

The client interacts with the server via a CLI. 

# Server

The server stores files in the local filesystem and file metadata in redis for quick access. The server expects a configuration file in the following format

