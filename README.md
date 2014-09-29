#bdbd
====
###简介
bdbd是采用redis作为通讯协议，bdb作为存储引擎的服务器程序

* bdb介绍
> - BDB是Oracle的一个开源kv数据库引擎，该引擎提供了高性能的kv数据读写，丰富的数据结构，事务，高可用性等特性。

* 为何选择bdb，而不直接使用redis:
> - 数据持久化，海量数据情况下，不必占用过多服务器内存，可以快速重启服务

* 为何选择bdb引擎，而不使用leveldb引擎:
> - 高可用性，引擎支持一主多从的复制和自动切换
> - 支持事务，完整的ACID支持
> - 丰富的数类型，除了HASH外，还支持BTREE,QUEUE等数据结构
> - 可选择Oracle商用许可证，以获得服务支持


###编译

```shell
#解压bdb源代码
tar xzvf db-6.1.19.tar.gz
编译
====
#解压bdb源代码
tar xzvf db-6.1.19.tar.gz
#编译bdb
cd db-6.1.19/build_unix
../dist/configure --prefix=$PWD
make -j4
make install
#编译bdbd
GOPATH=$PWD CGO_CFLAGS="-I$PWD/include" CGO_LDFLAGS="-L$PWD" go get github.com/nybuxtsui/bdbd/bdbd
#复制默认bdbd配置文
cp src/github.com/nybuxtsui/bdbd/bdbd.conf bin
#复制默认bdb配置文件，根据服务器状况调整BDB内存参数或其他参数
mkdir bin/db1
cp src/github.com/nybuxtsui/bdbd/DB_CONFIG bin/db1
#运行bdbd
cd bin
./bdbd
```
DB_CONFIG的详细配置信息参见[ \[Oracle文档\]](http://docs.oracle.com/cd/E17076_04/html/api_reference/CXX/configuration_reference.html):
