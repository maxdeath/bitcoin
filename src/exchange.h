// Copyright (c) 2018 The Abcmint developers

#ifndef ABCMINT_EXCHANGE_H
#define ABCMINT_EXCHANGE_H

#include "primitives/block.h"


#include <boost/thread.hpp>
#include <mysql/mysql.h>
#include <mysql/errmsg.h>


static const unsigned int KEY_POOL_SIZE         = 100;


/*use map to serialize to Berkeley DB, not unordered_map
  the user define std map, use red-black tree inside, defind the compare funcion
  can't use userid/tranaction id as key, because:
  1, in one transaction, it can charge for more than one user
  2, more than one transaction charge for one user
*/
struct comp
{
    typedef std::pair<unsigned int, std::string> int_string;
    bool operator () (const int_string & ls, const int_string &rs)
    {
        return ls.first < rs.first || (ls.first == rs.first && ls.second < rs.second);
    }

};

//key:<userid, transaction id>
//value:<charge value, status>, status is false at first, if already send to exchange server, the status change to true
typedef std::map<std::pair<unsigned int, std::string>, std::pair<int64_t, bool>, comp> value_type;
static std::map<uint256, value_type> chargeMap;


MYSQL *ConnectMysql();
bool LoadDepositAddress();
bool UpdateMysqlBalance(const CBlock *block, bool add);
void FillKeyPool(boost::thread_group& threadGroup);
void UpdateBalance(boost::thread_group& threadGroup);

void ScanAddress();




#endif

