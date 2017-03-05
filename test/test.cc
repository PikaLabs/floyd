#include"floyd_db.h"
#include"floyd_db.cc"
#include<iostream>
#include<sstream>
#include"include/slash_status.h"
#include"floyd_mutex.cc"
using namespace std;
using slash::Status;
int main(){
  
    floyd::Status s ;
    s = floyd::Status::NotFound("1","2");
    cout<<s.ToString()<<endl;
    s = floyd::Status::IOError("1","2");
    cout<<s.ToString()<<endl;
  /* floyd statu test
 * ok
 * not found
 * corrorp
 * not support
 * invalid argument
 * io error
 * end file
 *
    leveldb::Status leveldb_status = leveldb::Status::OK();
    s = leveldb_status;
    cout<<"status-test:ok"<<endl<<s.ToString()<<endl;
    cout<<"============================================"<<endl;
    leveldb_status = leveldb::Status::NotFound("msg1","msg2");
    s =static_cast<floyd::Status>( leveldb_status);
    cout<<"status-test:notfound"<<endl<<s.ToString()<<endl;
    cout<<"============================================"<<endl;
    leveldb_status = leveldb::Status::Corruption("msg1","msg2");
    s =static_cast<floyd::Status>( leveldb_status);
    cout<<"status-test:corrupt"<<endl<<s.ToString()<<endl;
    cout<<"============================================"<<endl;
    leveldb_status = leveldb::Status::NotSupported("msg1","msg2");
    s =static_cast<floyd::Status>( leveldb_status);
    cout<<"status-test:not support"<<endl<<s.ToString()<<endl;
    cout<<"============================================"<<endl;
    leveldb_status = leveldb::Status::InvalidArgument("msg1","msg2");
    s =static_cast<floyd::Status>( leveldb_status);
    cout<<"status-test:invalid argument"<<endl<<s.ToString()<<endl;
    cout<<"============================================"<<endl;
    leveldb_status = leveldb::Status::IOError("msg1","msg2");
    s =static_cast<floyd::Status>( leveldb_status);
    cout<<"status-test:io error"<<endl<<s.ToString()<<endl;
    cout<<"============================================"<<endl;
 */
 /* floyd_db test
    floyd::Db_backend *a =new  floyd::Leveldb_backend("/home/wangwenduo/data");
    s = a->start(); 
    string key("ha1a"); 
    stringstream sstream;
    string val1;
    string val2;
    for(int i=1;i<=100;i++){
        sstream<<i;
        val1=sstream.str();
        sstream.str("");
        a->set(key,val1);
        cout<<"val1:"<<val1<<endl;
        s = a->get(key,val2);
        cout<<"status:"<<s.ToString()<<endl;
        cout<<"value:"<<val2<<endl;
    }
 */
 /* floyd_meta test
    NodeInfo a=NodeInfo("1",2);
    NodeInfo b=NodeInfo("1",3);
    NodeInfo c=NodeInfo("2",2);
    NodeInfo d=NodeInfo("1",2);
    cout<<(a==b)<<(a==d)<<(a==c)<<endl;
 */
    return 0;
}
