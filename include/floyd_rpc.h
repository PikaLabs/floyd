#ifndef FLOYD_RPC_H_
#define FLOYD_RPC_H_

#include "floyd_meta.h"
//#include "floyd.h"

namespace floyd {

Status Rpc(NodeInfo* ni, command::Command& cmd, command::CommandRes& cmd_res);
command::Command BuildWriteCommand(const std::string& key,
                                   const std::string& value);
command::Command BuildReadAllCommand();
command::Command BuildReadCommand(const std::string& key);
command::Command BuildDeleteCommand(const std::string& key);

}
#endif
