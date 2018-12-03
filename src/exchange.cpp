#include "exchange.h"
#include "script/script.h"
#include "script/standard.h"
#include "util/time.h"
#include "util/strencodings.h"
#include "util/system.h"
#include "uint256.h"
#include "validation.h"
#include "chainparams.h"
#include "primitives/transaction.h"
#include "logging.h"
#include "wallet/wallet.h"
#include "pubkey.h"
#include "key_io.h"
#include "rpc/protocol.h"

#include "jansson.h"

#include <string>
#include <map>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ip/v6_only.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>




static const unsigned int CHARGE_MATURITY       = 6;
static const unsigned int HISTORY_TABLE_COUNT   = 100;
static const char*        UPDATE_COMMAND        = "balance.update";
static const char*        BALANCE_ASSET         = "BTC";
static const char*        BALANCE_BUSINESS      = "deposit";

static MYSQL mysql;
static std::map<unsigned int, CKeyID> depositKeyIdMap;


MYSQL *ConnectMysql()
{
    MYSQL *conn = mysql_init(&mysql);
    if (conn == NULL)
        return NULL;

    my_bool reconnect = 1;
    if (mysql_options(conn, MYSQL_OPT_RECONNECT, &reconnect) != 0) {
        mysql_close(conn);
        return NULL;
    }

    if (mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8") != 0) {
        mysql_close(conn);
        return NULL;
    }

    std::string strHost =   gArgs.GetArg("-host", "host");
    std::string strUser =   gArgs.GetArg("-user", "user");
    std::string strPasswd = gArgs.GetArg("-passwd", "passwd");
    std::string strDbname = gArgs.GetArg("-dbname", "dbname");
    int64_t dbport = gArgs.GetArg("-dbport", 3306);
    if (mysql_real_connect(conn, strHost.c_str(), strUser.c_str(), strPasswd.c_str(), strDbname.c_str(), dbport, NULL, 0) == NULL) {
        mysql_close(conn);
        return NULL;
    }

    LogPrintf("exchange, connect to mysql, success\n");

    return conn;
}

bool LoadDepositAddress()
{
    char query[64];
    sprintf(query, "select * from `abcmint`.`coin_btc`");
    int ret = mysql_query(&mysql, query);
    if(ret != 0) {
        LogPrintf("exchange, Query failed (%s)\n",mysql_error(&mysql));
        return false;
    }

    MYSQL_RES *res;
    if (!(res=mysql_store_result(&mysql)))
    {
        LogPrintf("exchange, Couldn't get result from %s\n", mysql_error(&mysql));
        return false;
    }

    size_t num_rows = mysql_num_rows(res);
    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(res);
        unsigned int userId = strtoul(row[0], NULL, 10);

        //support P2PKH only
        CTxDestination dest = DecodeDestination(row[1]);
        CKeyID* pKeyId = boost::get<CKeyID>(&dest);
        if (NULL!= pKeyId) {
            depositKeyIdMap[userId] = *pKeyId;
        }

    }

    mysql_free_result(res);
    return true;
}

/*the business_id should be use to identify the charge history, because the block can be connect-disconnect-reconnect.
one case below, A is the root block:
step 1, B/C block come and both link to block A， A-B becomes the best chain, B block will be connect and send charge
record to exchange server.
 ___    ___
|_A_|->|_B_|                <-------- the best chain
  |     ___
  |--->|_C_|

step 2, D block comes and links to C block, A-C-D becomes the best chain, B would be disconnect, C and D would be
connect and send charge record to exchange server.
 ___    ___
|_A_|->|_B_|
  |     ___    ___
  |--->|_C_|->|_D_|         <-------- the best chain

step3, E and F block comes and connect to block B, A-B-E-F becomes the best chain. B will be reconnect, but if
business_id is not used, we can't identify the charge history, and the charge record in B won't be send to exchange
server.
 ___    ___    ___    ___
|_A_|->|_B_|->|_E_|->|_F_|  <-------- the best chain
  |     ___    ___
  |--->|_C_|->|_D_|

*/
bool GetBalanceHistory(const unsigned int userId, const std::string& txId, int64_t& chargeBusinessId)
{
    char query[256];
    sprintf(query, "select * from `trade_history`.`balance_history_%u`  WHERE `user_id` = %u and `detail` like '%%%s%%'",
            userId % HISTORY_TABLE_COUNT, userId, txId.c_str());
    int ret = mysql_query(&mysql, query);
    if(ret != 0) {
        LogPrintf("exchange, GetBalanceHistory Query failed (%s)\n",mysql_error(&mysql));
        return false;
    }

    MYSQL_RES *res;
    if (!(res=mysql_store_result(&mysql)))
    {
        LogPrintf("exchange, GetBalanceHistory Couldn't get result from %s\n", mysql_error(&mysql));
        return false;
    }

    std::vector<int64_t> vConnect;
    std::vector<int64_t> vDisconnect;
    size_t num_rows = mysql_num_rows(res);
    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(res);
        //printf("exchange, GetBalanceHistory row[7] is null:%s\n", NULL == row[7]?"true":"false");
        json_t *detail = json_loads(row[7], 0, NULL);
        if (!detail) {
            LogPrintf("exchange, GetBalanceHistory invalid balance history record : %s\n", row[7]);
            return false;
        }

        //if find disconnect block, ignore the id
        json_t * disconnect_business_id = json_object_get(detail,"disconnect");
        if (json_is_integer(disconnect_business_id)) {
            int64_t disconnectBusinessId = json_integer_value(disconnect_business_id);
            vDisconnect.push_back(disconnectBusinessId);
            json_decref(detail);
            continue;
        }

        //no disconnect detail, means connect block
        json_t * connect_business_id = json_object_get(detail, "id");
        if (json_is_integer(connect_business_id)) {
            int64_t connectBusinessId = json_integer_value(connect_business_id);
            vConnect.push_back(connectBusinessId);
        }

        json_decref(detail);
    }

    unsigned int connectCount = vConnect.size();
    unsigned int disconnectCount = vDisconnect.size();
    for ( auto itr=vConnect.begin(); itr!=vConnect.end(); ) {
        bool find = false;
        for ( auto itr_dis=vDisconnect.begin(); itr_dis!=vDisconnect.end(); ) {
            if ( *itr == *itr_dis ) {
                itr = vConnect.erase(itr);
                itr_dis = vDisconnect.erase(itr_dis);
                find = true;
                break;
            }
            else
            {
                ++itr_dis;
            }
        }
        if (!find) ++itr;
    }

    if ((vConnect.size() != 0 && vConnect.size() != 1) || vDisconnect.size() != 0) {
        LogPrintf("exchange, mysql GetBalanceHistory， balance_history_%u in error state, connect times should be equal "
        "or one more time than disconnect, connectCount:%u, disconnectCount:%u, userid:%u\n",
        userId % HISTORY_TABLE_COUNT, connectCount, disconnectCount, userId);
        return false;
    }
    chargeBusinessId = vConnect.size() == 0 ? 0:vConnect[0];
    return true;
}


//
// IOStream device that speaks SSL but can also speak non-SSL
//
template <typename Protocol>
class SSLIOStreamDevice : public boost::iostreams::device<boost::iostreams::bidirectional> {
public:
    SSLIOStreamDevice(boost::asio::ssl::stream<typename Protocol::socket> &streamIn, bool fUseSSLIn) : stream(streamIn)
    {
        fUseSSL = fUseSSLIn;
        fNeedHandshake = fUseSSLIn;
    }

    void handshake(boost::asio::ssl::stream_base::handshake_type role)
    {
        if (!fNeedHandshake) return;
        fNeedHandshake = false;
        stream.handshake(role);
    }
    std::streamsize read(char* s, std::streamsize n)
    {
        handshake(boost::asio::ssl::stream_base::server); // HTTPS servers read first
        if (fUseSSL) return stream.read_some(boost::asio::buffer(s, n));
        return stream.next_layer().read_some(boost::asio::buffer(s, n));
    }
    std::streamsize write(const char* s, std::streamsize n)
    {
        handshake(boost::asio::ssl::stream_base::client); // HTTPS clients write first
        if (fUseSSL) return boost::asio::write(stream, boost::asio::buffer(s, n));
        return boost::asio::write(stream.next_layer(), boost::asio::buffer(s, n));
    }
    bool connect(const std::string& server, const std::string& port)
    {
        boost::asio::ip::tcp::resolver resolver(stream.get_io_service());
        boost::asio::ip::tcp::resolver::query query(server.c_str(), port.c_str());
        boost::asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        boost::asio::ip::tcp::resolver::iterator end;
        boost::system::error_code error = boost::asio::error::host_not_found;
        while (error && endpoint_iterator != end)
        {
            stream.lowest_layer().close();
            stream.lowest_layer().connect(*endpoint_iterator++, error);
        }
        if (error)
            return false;
        return true;
    }

private:
    bool fNeedHandshake;
    bool fUseSSL;
    boost::asio::ssl::stream<typename Protocol::socket>& stream;
};

//
// HTTP protocol
//
// This ain't Apache.  We're just using HTTP header for the length field
// and to be compatible with other JSON-RPC implementations.
//
typedef const std::map<std::string, std::string>::value_type const_pair;
std::string HTTPPost(const std::string& strMsg, const std::map<std::string, std::string>& mapRequestHeaders)
{
    std::ostringstream s;
    s << "POST / HTTP/1.1\r\n"
      << "User-Agent: abcmint-json-rpc/" << FormatFullVersion() << "\r\n"
      << "Host: 127.0.0.1\r\n"
      << "Content-Type: application/json\r\n"
      << "Content-Length: " << strMsg.size() << "\r\n"
      << "Connection: close\r\n"
      << "Accept: application/json\r\n";
    BOOST_FOREACH(const_pair & item, mapRequestHeaders)
        s << item.first << ": " << item.second << "\r\n";
    s << "\r\n" << strMsg;

    return s.str();
}

int ReadHTTPStatus(std::basic_istream<char>& stream, int &proto)
{
    std::string str;
    getline(stream, str);
    std::vector<std::string> vWords;
    boost::split(vWords, str, boost::is_any_of(" "));
    if (vWords.size() < 2)
        return HTTP_INTERNAL_SERVER_ERROR;
    proto = 0;
    const char *ver = strstr(str.c_str(), "HTTP/1.");
    if (ver != NULL)
        proto = atoi(ver+7);
    return atoi(vWords[1].c_str());
}

int ReadHTTPHeaders(std::basic_istream<char>& stream, std::map<std::string, std::string>& mapHeadersRet)
{
    int nLen = 0;
    while(true)
    {
        std::string str;
        getline(stream, str);
        if (str.empty() || str == "\r")
            break;
        std::string::size_type nColon = str.find(":");
        if (nColon != std::string::npos)
        {
            std::string strHeader = str.substr(0, nColon);
            boost::trim(strHeader);
            boost::to_lower(strHeader);
            std::string strValue = str.substr(nColon+1);
            boost::trim(strValue);
            mapHeadersRet[strHeader] = strValue;
            if (strHeader == "content-length")
                nLen = atoi(strValue.c_str());
        }
    }
    return nLen;
}

int ReadHTTPMessage(std::basic_istream<char>& stream, std::map<std::string, std::string>& mapHeadersRet, std::string& strMessageRet, int nProto)
{
    mapHeadersRet.clear();
    strMessageRet = "";

    // Read header
    int nLen = ReadHTTPHeaders(stream, mapHeadersRet);
    if (nLen < 0 || nLen > (int)MAX_SIZE)
        return HTTP_INTERNAL_SERVER_ERROR;

    // Read message
    if (nLen > 0)
    {
        std::vector<char> vch(nLen);
        stream.read(&vch[0], nLen);
        strMessageRet = std::string(vch.begin(), vch.end());
    }

    std::string sConHdr = mapHeadersRet["connection"];

    if ((sConHdr != "close") && (sConHdr != "keep-alive"))
    {
        if (nProto >= 1)
            mapHeadersRet["connection"] = "keep-alive";
        else
            mapHeadersRet["connection"] = "close";
    }

    return HTTP_OK;
}


bool CallExchangeServer(std::string strRequest)
{
    // Connect to localhost
    boost::asio::io_service io_service;
    boost::asio::ssl::context context(io_service, boost::asio::ssl::context::sslv23);
    context.set_options(boost::asio::ssl::context::no_sslv2);
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket> sslStream(io_service, context);
    SSLIOStreamDevice<boost::asio::ip::tcp> d(sslStream, false);
    boost::iostreams::stream< SSLIOStreamDevice<boost::asio::ip::tcp> > stream(d);
    if (!d.connect(gArgs.GetArg("-exchangeserver", "127.0.0.1"), gArgs.GetArg("-exchangeport", "8080")))
        throw std::runtime_error("couldn't connect to exchange server");

    // Send request
    std::map<std::string, std::string> mapRequestHeaders;
    std::string strPost = HTTPPost(strRequest, mapRequestHeaders);
    stream << strPost << std::flush;

    // Receive HTTP reply status
    int nProto = 0;
    ReadHTTPStatus(stream, nProto);

    // Receive HTTP reply message headers and body
    std::map<std::string, std::string> mapHeaders;
    std::string strReply;
    ReadHTTPMessage(stream, mapHeaders, strReply, nProto);

    if (strReply.empty()) {
        printf("no response from server!\n");
        return false;
    }

    // Parse response
    json_t *response = json_loads(strReply.c_str(), 0, NULL);
    if (json_is_null(response)) {
        printf("couldn't parse response from server!\n");
        json_decref(response);
        return false;
    }

    json_t * error = json_object_get(response, "error");
    if (error != json_null()) {
        printf("expected error to be null, but not!\n");
        json_decref(response);
        return false;
    }

    json_t * result = json_object_get(response, "result");
    json_t * status = json_object_get(result, "status");
    if (!json_is_string(status) || 0 != strcmp(json_string_value(status), "success")) {
        printf("expected status to be success, but not!\n");
        json_decref(response);
        return false;
    }

    json_decref(response);
    return true;

}

bool SendUpdateBalance(const unsigned int& userId, const std::string& txId, const int64_t& valueIn,
                            bool connect, const int64_t& chargeBusinessId)
{
    //{"id":5,"method":"account.update",params": [1, "BTC", "deposit", 100, "1.2345"]}
    json_t * request = json_object();
    json_object_set_new(request, "id", json_integer(userId));//json_integer user long long int
    json_object_set_new(request, "method", json_string(UPDATE_COMMAND));

    json_t * params = json_array();
    json_array_append_new(params, json_integer(userId));               // user_id
    json_array_append_new(params, json_string(BALANCE_ASSET));         // asset
    json_array_append_new(params, json_string(BALANCE_BUSINESS));      // business

    int64_t business_id = GetTimeMicros();
    json_array_append_new(params, json_integer(business_id));          // business_id, use for repeat checking

    //convert to char*
    int64_t value = (valueIn > 0 ? valueIn : -valueIn);
    int64_t quotient = value/COIN;
    int64_t remainder = value % COIN;

    char value_str[64];
    sprintf(value_str, "%s%lld%s%08lld", (((connect && valueIn > 0) ||(!connect && valueIn < 0)) ? "+" : "-"), quotient, ".", remainder);
    json_array_append_new(params, json_string(value_str));                // change

    //detail
    json_t * detail = json_object();
    if(!connect) json_object_set_new(detail, "disconnect", json_integer(chargeBusinessId));
    json_object_set_new(detail, "txId", json_string(txId.c_str()));
    json_array_append_new(params, detail);
    json_object_set_new(request, "params", params);

    char* request_str = json_dumps(request, 0);
    std::string strRequest(request_str);

    bool result = CallExchangeServer(strRequest);
    free(request_str);
    json_decref(request);
    return result;

}

bool UpdateMysqlBalanceConnect(const uint256& hash, value_type& chargeRecord)
{
    for (value_type::iterator it = chargeRecord.begin(); it != chargeRecord.end(); ++it) {
        const std::pair<unsigned int, std::string> & key = it->first;
        const unsigned int& userId = key.first;
        const std::string& txId = key.second;

        std::pair<int64_t, bool>& value = it->second;
        int64_t& chargeValue = value.first;
        bool& status = value.second;

        if (status) continue;

        int64_t chargeBusinessId;
        if (!GetBalanceHistory(userId, txId, chargeBusinessId)) {
            LogPrintf("exchange UpdateMysqlBalanceConnect GetBalanceHistory return false, userId:%u, txId:%s, chargeBusinessId:%lld\n",
                userId, txId.c_str(),chargeBusinessId);
            continue;
        }

        if (0 == chargeBusinessId) {
            if (!SendUpdateBalance(userId, txId, chargeValue, true, chargeBusinessId)) {
                LogPrintf("exchange UpdateMysqlBalanceConnect exchange, SendUpdateBalance failed, chargeBusinessId:%lld, userId:%u, txId:%s, chargeValue:%lld, add:true\n",
                    chargeBusinessId, userId, txId.c_str(), chargeValue);
                status = false;
                continue;
            } else
                status = true;
        } else {
            status = true;
            continue;
        }
    }

    //over-write to update the status
    std::vector<std::shared_ptr<CWallet>> wallets = GetWallets();
    std::shared_ptr<CWallet> const wallet = (wallets.size() == 1 ? wallets[0] : nullptr);
    CWallet* const pwallet = wallet.get();

    if (NULL == pwallet || !pwallet->AddChargeRecordInOneBlock(hash, chargeRecord)) {
        LogPrintf("exchange, wallet is null or update status for block %s in berkeley db return false!\n", hash.ToString().c_str());
        return false;
    }

    return true;
}

bool UpdateMysqlBalanceDisConnect(const uint256& hash, value_type& chargeRecord)
{
    for (value_type::iterator it = chargeRecord.begin(); it != chargeRecord.end(); ++it) {
        const std::pair<unsigned int, std::string> & key = it->first;
        const unsigned int& userId = key.first;
        const std::string& txId = key.second;

        std::pair<int64_t, bool>& value = it->second;
        int64_t& chargeValue = value.first;

        int64_t chargeBusinessId;
        if (!GetBalanceHistory(userId, txId, chargeBusinessId)) return false;
        if (0 != chargeBusinessId) {
            if (!SendUpdateBalance(userId, txId, chargeValue, false, chargeBusinessId)) {
                LogPrintf("exchange, UpdateMysqlBalanceDisConnect SendUpdateBalance failed, chargeBusinessId:%lld, userId:%u, txId:%s, chargeValue:%lld, add:true\n",
                    chargeBusinessId, userId, txId.c_str(), chargeValue);
                return false;
            }
        } else {
            continue;
        }
    }

    std::vector<std::shared_ptr<CWallet>> wallets = GetWallets();
    std::shared_ptr<CWallet> const wallet = (wallets.size() == 1 ? wallets[0] : nullptr);
    CWallet* const pwallet = wallet.get();

    if (NULL == pwallet || !pwallet->DeleteChargeRecordInOneBlock(hash)) {
        LogPrintf("exchange, wallet is null or disconnect block %s failed, delete charge record in berkeley db return false!\n", hash.ToString().c_str());
        return false;
    }

    return true;
}

bool UpdateMysqlBalance(const CBlock *block, bool add)
{
    LOCK(cs_main);
    //load the address each time when update balance, it would be better the front end notify to add or load address.
    if (!LoadDepositAddress()){
        LogPrintf("exchange, connect to query table coin_btc! please check mysql abcmint database.\n");
        return false;
    }

    if (add) {
        value_type chargeMapOneBlock;
        unsigned int nTxCount = block->vtx.size();
        for (unsigned int i=0; i<nTxCount; i++)
        {
            const CTransaction &tx = *(block->vtx[i]);
            //if(tx.IsCoinBase()) continue;

            unsigned int nVoutSize = tx.vout.size();
            for (unsigned int i = 0; i < nVoutSize; i++) {
                const CTxOut &txOut = tx.vout[i];

                std::vector<std::vector<unsigned char> > vSolutions;
                txnouttype whichType = Solver(txOut.scriptPubKey, vSolutions);
                if (TX_NONSTANDARD == whichType || TX_WITNESS_UNKNOWN == whichType) {
                    LogPrintf("exchange UpdateMysqlBalance Solver whichType is unknown %d\n", whichType);
                    continue;
                }

                CKeyID keyID;
                if (TX_PUBKEYHASH == whichType) {
                    keyID = CKeyID(uint160(vSolutions[0]));
                    std::string addr = EncodeDestination(keyID);
                } else {
                    continue;
                }


                for (auto& x: depositKeyIdMap) {
                    if (keyID == x.second) {
                        std::pair<unsigned int, std::string> key = std::make_pair(x.first, tx.GetHash().ToString());
                        std::pair<int64_t, bool>& value = chargeMapOneBlock[key];
                        value.first += txOut.nValue;
                        value.second = false;
                        break; //address is unique, user and address, 1:1
                    }
                }
            }
        }

        /*connect new block
          check CHARGE_MATURITY block before and call exchange server to commit to mysql
          before that, check if the record is already exists in mysql, for start with re-index option*/
        if (!chargeMapOneBlock.empty()) {
            std::vector<std::shared_ptr<CWallet>> wallets = GetWallets();
            std::shared_ptr<CWallet> const wallet = (wallets.size() == 1 ? wallets[0] : nullptr);
            CWallet* const pwallet = wallet.get();
            if (NULL == pwallet || !pwallet->AddChargeRecordInOneBlock(block->GetHash(), chargeMapOneBlock)) {
                LogPrintf("exchange, wallet is null or connect new block %s failed, add charge record to berkeley db return false!\n",
                    block->GetHash().ToString().c_str());
                return false;
            }
            chargeMap[block->GetHash()] = chargeMapOneBlock;
        }

        //get six block before
        int nMaturity = CHARGE_MATURITY;
        BlockMap::iterator mi = mapBlockIndex.find(block->GetHash());
        assert(mi != mapBlockIndex.end());
        CBlockIndex* pBlockIndex = mi->second;
        while (--nMaturity > 0 && pBlockIndex) {
            pBlockIndex = pBlockIndex->pprev;
        }

        //if pBlockIndex is null, return true
        if (!pBlockIndex) return true;

        std::map<uint256, value_type>::iterator got = chargeMap.find(pBlockIndex->GetBlockHash());
        if (got == chargeMap.end()) {
            return true;
        }

        const uint256& hash = got->first;
        value_type& chargeRecord = got->second;
        return UpdateMysqlBalanceConnect(hash, chargeRecord);

    } else {
        /*disconnect block, check if the record is already exists in mysql, minus the value*/
        std::map<uint256, value_type>::iterator got = chargeMap.find(block->GetHash());
        if (got == chargeMap.end()) {
            return true;
        }

        const uint256& hash = got->first;
        value_type& chargeRecord = got->second;
        if (UpdateMysqlBalanceDisConnect(hash, chargeRecord)) {
            chargeMap.erase(hash);
            return true;
        } else
            return false;
    }
}


void charge()
{
    LogPrintf("exchange, charge thread started\n");
    RenameThread("charge");

    try {
        while (true) {
            if (fImporting || fReindex) {
                MilliSleep(60*1000);//1 minutes
                continue;
            }

            LOCK(cs_main);

            for (std::map<uint256, value_type>::iterator it= chargeMap.begin(); it!= chargeMap.end();) {
                const uint256& hash = it->first;
                value_type& chargeRecord = it->second;
                BlockMap::iterator mi = mapBlockIndex.find(hash);
                assert(mi != mapBlockIndex.end());
                CBlockIndex* pBlockIndex = mi->second;

                CBlockIndex *pnext = chainActive.Next(pBlockIndex);
                if(NULL != pnext || pindexBestHeader == pBlockIndex){
                    //in main chain, connect
                    if (pindexBestHeader->nHeight - pBlockIndex->nHeight >5) {
                        UpdateMysqlBalanceConnect(hash, chargeRecord);
                    }
                    ++it;
                } else {
                    //not in main chain, disconnect
                    if (UpdateMysqlBalanceDisConnect(hash, chargeRecord)) {
                        chargeMap.erase(hash);
                    } else
                        ++it;
                }
            }

            if (chargeMap.size() == 0) {
                MilliSleep(30*60*1000);//30 minutes
            }
        }
    }
    catch (boost::thread_interrupted)
    {
        LogPrintf("exchange, charge thread terminated\n");
        throw;
    }
}


void UpdateBalance(boost::thread_group& threadGroup)
{
    //use boost thread group, so that this thread can exit together with other thread when press ctrl+c
    //threadGroup.create_thread(boost::bind(&charge));
}


void AddressScanner()
{
    LogPrintf("exchange, AddressScanner started\n");
    RenameThread("AddressScanner");

    const CChainParams& chainparams = Params();
    BlockMap::iterator mi = mapBlockIndex.find(chainparams.GetConsensus().hashGenesisBlock);
    assert(mi != mapBlockIndex.end());
    CBlockIndex* pindexGenesisBlock = mi->second;
    CBlockIndex* pBlockIterator = pindexGenesisBlock;
    try {
        while (true) {
            if (fImporting || fReindex) {
                MilliSleep(60*1000);//1 minutes
                continue;
            }

            if(pBlockIterator ==NULL) {
                MilliSleep(60*1000);//1 minutes
                pBlockIterator = pindexGenesisBlock;
                continue;
            }

            if(pindexBestHeader == pBlockIterator) {
                MilliSleep(10*60*1000);//10 minutes
                continue;
            }

            LOCK(cs_main);
            CBlock block;
            ReadBlockFromDisk(block, pBlockIterator, chainparams.GetConsensus());

            unsigned int nTxCount = block.vtx.size();
            LogPrintf("exchange AddressScanner process block %u, transaction count:%u\n", pBlockIterator->nHeight, nTxCount);
            for (unsigned int i=0; i<nTxCount; i++)
            {
                const CTransaction &tx = *(block.vtx[i]);
                //if(tx.IsCoinBase()) continue;

                unsigned int nVoutSize = tx.vout.size();
                for (unsigned int i = 0; i < nVoutSize; i++) {
                    const CTxOut &txOut = tx.vout[i];
                    //-------------------------------------step 1, get user address-------------------------------------
                    std::vector<std::vector<unsigned char> > vSolutions;
                    txnouttype whichType = Solver(txOut.scriptPubKey, vSolutions);
                    if (TX_NONSTANDARD == whichType || TX_WITNESS_UNKNOWN == whichType) {
                        LogPrintf("exchange AddressScanner Solver whichType is unknown %d\n", whichType);
                        continue;
                    }

                    LogPrintf("exchange AddressScanner find address, type:%u\n", whichType);

                    CKeyID keyID;
                    std::string addr;
                    if (TX_PUBKEYHASH == whichType) {
                        keyID = CKeyID(uint160(vSolutions[0]));
                        addr = EncodeDestination(keyID);
                    } else {
                        continue;
                    }

                    //-------------------------------------step 2, check if the address exists-------------------------------------
                    char query[256];
                    sprintf(query, "select * from `abcmint`.`coin_btc` where `address` = '%s'", addr.c_str());
                    int ret = mysql_query(&mysql, query);
                    if(ret != 0) {
                        LogPrintf("exchange, Query failed (%s)\n",mysql_error(&mysql));
                        continue;
                    }

                    MYSQL_RES *res;
                    if (!(res=mysql_store_result(&mysql)))
                    {
                        LogPrintf("exchange, Couldn't get result from %s\n", mysql_error(&mysql));
                        continue;
                    }

                    size_t num_rows = mysql_num_rows(res);
                    mysql_free_result(res);
                    if(num_rows > 0) continue;

                    //-------------------------------------step 3, create a user-------------------------------------
                    char insert_user[512];
                    sprintf(insert_user, "INSERT INTO `abcmint`.`abc_user`(`nickname`, `email`, `phone`, `passwd`, `country`, `balance`, `freeze`, `active`, `permit`, `register_time`)"
                    " VALUES ('true', 'true@qq.com', '123456789', 'false', NULL, NULL, NULL, NULL, NULL, '2018-11-01 17:38:06')");
                    ret = mysql_query(&mysql, insert_user);
                    if(ret != 0) {
                        LogPrintf("exchange, insert user failed (%s)\n",mysql_error(&mysql));
                        continue;
                    }

                    //-------------------------------------step 4, get the user just created-------------------------------------
                    char query_user[256];
                    sprintf(query_user, "select max(id) from `abcmint`.`abc_user`");
                    ret = mysql_query(&mysql, query_user);
                    if(ret != 0) {
                        LogPrintf("exchange, Query abc_user failed (%s)\n",mysql_error(&mysql));
                        continue;
                    }

                    if (!(res=mysql_store_result(&mysql)))
                    {
                        LogPrintf("exchange, Couldn't get result from abc_user %s\n", mysql_error(&mysql));
                        continue;
                    }

                    num_rows = mysql_num_rows(res);
                    if(num_rows < 1) {
                        mysql_free_result(res);
                        continue;
                    }
                    MYSQL_ROW row = mysql_fetch_row(res);

                    //-------------------------------------step 5, insert the user address-------------------------------------
                    char insert_address[512];
                    sprintf(insert_address, "INSERT INTO `abcmint`.`coin_btc`(`id`, `address`, `type`) VALUES (%s, '%s', %d)",
                    row[0], addr.c_str(), whichType);
                    ret = mysql_query(&mysql, insert_address);
                    if(ret != 0) {
                        LogPrintf("exchange, insert address failed (%s)\n",mysql_error(&mysql));
                        mysql_free_result(res);
                        continue;
                    }

                    mysql_free_result(res);
                }
            }
            CBlockIndex *pnext = chainActive.Next(pBlockIterator);
            pBlockIterator = pnext;
        }
    }
    catch (boost::thread_interrupted)
    {
        LogPrintf("exchange, AddressScanner terminated\n");
        throw;
    }
}


void ScanAddress()
{
    static boost::thread_group* scanThreads = NULL;

    int nThreads = gArgs.GetArg("-scanproclimit", -1);
    if (nThreads < 0)
        nThreads = boost::thread::hardware_concurrency();

    if (scanThreads != NULL)
    {
        scanThreads->interrupt_all();
        delete scanThreads;
        scanThreads = NULL;
    }

    if (nThreads == 0)
        return;

    scanThreads = new boost::thread_group();
    for (int i = 0; i < nThreads; i++) {
        scanThreads->create_thread(boost::bind(&AddressScanner));
    }
}

