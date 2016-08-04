#ifndef __FLOYD_RPC_H__
#define __FLOYD_RPC_H__

#include "floyd.h"
namespace floyd {
Status Rpc(NodeInfo* ni, command::Command& cmd, command::CommandRes& cmd_res);
command::Command BuildWriteCommand(const std::string& key,
                                   const std::string& value);
command::Command BuildReadAllCommand();
command::Command BuildReadCommand(const std::string& key);
command::Command BuildDeleteCommand(const std::string& key);
}
#endif
