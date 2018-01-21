/*
 * hiredispp.h
 */

#ifndef HIREDISPP_H
#define HIREDISPP_H

#include <string.h>
#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/optional.hpp>
#include <hiredis/hiredis.h>

namespace hiredispp
{
    template<typename CharT>
    class RedisEncoding
    {
    public:
        typedef std::basic_string<CharT> RedisString;

        static void decode(const std::string& data, RedisString& string);
        static void decode(const char* data, size_t size, RedisString& string);

        static void encode(const RedisString& string, std::string& data);
        static void encode(const RedisString& string, std::ostream& data);
    };

    class RedisException : public std::exception
    {
        std::string _what;

    public:
        RedisException(const char* cstr)
            : _what(cstr) { }
        RedisException(const std::string& what)
            : _what(what) { }

        virtual ~RedisException() throw() { }

        virtual const char* what() const throw()
        {
            return _what.c_str();
        }
    };

    class RedisElementBase
    {
        redisReply* _r;

    public:
        RedisElementBase(redisReply* r)
            : _r(r) { }

        RedisElementBase(const RedisElementBase& from)
            : _r(from._r) {}

        RedisElementBase& operator=(const RedisElementBase& from)
        {
            _r = from._r;
            return *this;
        }

        virtual ~RedisElementBase()
        {
            _r = 0;
        }

        redisReply* get() const
        {
            return _r;
        }
    };

    template<typename CharT>
    class RedisConst
    {
    public:
        static const std::basic_string<CharT> Nil;
        static const std::basic_string<CharT> InfoSeparator;
        static const std::basic_string<CharT> InfoCrLf;
    };

    template<class T, typename CharT>
    class RedisResult : public T
    {
        std::basic_string<CharT> getString() const
        {
            std::basic_string<CharT> s;
            RedisEncoding<CharT>::decode(T::get()->str, T::get()->len, s);
            return s;
        }

    public:
        RedisResult(redisReply* r)
            : T(r) { }

        bool isError() const
        {
            return (T::get()->type == REDIS_REPLY_ERROR);
        }

        void checkError() const
        {
            if (isError())
            {
                RedisException e(std::string(T::get()->str, T::get()->len));

                throw e;
            }
        }

        bool isNil() const
        {
            return (T::get()->type == REDIS_REPLY_NIL);
        }

        std::basic_string<CharT> getErrorMessage() const
        {
            if (isError())
            {
                return getString();
            }

            return std::basic_string<CharT>();
        }

        std::basic_string<CharT> getStatus() const
        {
            checkError();

            if (T::get()->type != REDIS_REPLY_STATUS)
            {
                throw std::runtime_error("Invalid reply type");
            }

            return getString();
        }

        operator std::basic_string<CharT>() const
        {
            checkError();

            if (
                    T::get()->type != REDIS_REPLY_STRING &&
                    T::get()->type != REDIS_REPLY_NIL)
            {
                throw std::runtime_error("Invalid reply type");
            }

            if (isNil())
            {
                return RedisConst<CharT>::Nil;
            }

            return getString();
        }

        operator boost::int64_t() const
        {
            checkError();

            if (T::get()->type != REDIS_REPLY_INTEGER)
            {
                throw std::runtime_error("Invalid reply type");
            }

            return T::get()->integer;
        }

        operator boost::optional<boost::int64_t>() const
        {
            checkError();

            if (
                    T::get()->type != REDIS_REPLY_INTEGER &&
                    T::get()->type != REDIS_REPLY_NIL)
            {
                throw std::runtime_error("Invalid reply type");
            }

            if (isNil())
            {
                return boost::optional<boost::int64_t>();
            }

            return boost::optional<boost::int64_t>(T::get()->integer);
        }

        size_t size() const
        {
            checkError();
            
            if (T::get()->type != REDIS_REPLY_ARRAY)
            {
                throw std::runtime_error("Invalid reply type");
            }

            return T::get()->elements;
        }

        RedisResult<RedisElementBase, CharT> operator[](size_t i) const
        {
            checkError();

            if (T::get()->type != REDIS_REPLY_ARRAY)
            {
                throw std::runtime_error("Invalid reply type");
            }

            if (i >= T::get()->elements)
            {
                throw std::runtime_error("Out of range");
            }

            return RedisResult<RedisElementBase, CharT>(T::get()->element[i]);
        }

        template <class V>
        void toValue(V& v)
        {
            v = boost::lexical_cast<V>((std::basic_string<CharT>)(*this));
        }

        template <class V>
        void toVector(std::vector<V>& v)
        {
            for (size_t i = 0; i < size(); ++i)
            {
                v.push_back(boost::lexical_cast<V>((std::basic_string<CharT>)(*this)[i]));
            }
        }
    };

    class RedisReplyBase
    {
        redisReply* _r;
        int* _refs;

        void addRef()
        {
            ++(*_refs);
        }

        void release()
        {
            if (--(*_refs) == 0)
            {
                ::freeReplyObject(_r);
                delete _refs;
            }
        }

    public:
        RedisReplyBase(redisReply* r)
            : _r(r), _refs(new int(0))
        {
            addRef();
        }

        RedisReplyBase(const RedisReplyBase& from)
            : _r(from._r), _refs(from._refs)
        {
            addRef();
        }

        RedisReplyBase& operator=(const RedisReplyBase& from)
        {
            if (&from==this) {
                return *this;
            }

            release();

            _r = from._r;
            _refs = from._refs;

            addRef();

            return *this;
        }

        virtual ~RedisReplyBase()
        {
            release();

            _r = 0;
            _refs = 0;
        }

        redisReply* get() const
        {
            return _r;
        }
    };

    template<typename CharT>
    class RedisCommandBase
    {
        std::vector<std::string> _parts;

        void addPart(const std::basic_string<CharT>& s)
        {
            std::string data;
            RedisEncoding<CharT>::encode(s, data);
            _parts.push_back(data);
        }

        void addPart(const char* s)
        {
            std::string data(s, s + ::strlen(s));
            _parts.push_back(data);
        }

    public:
        RedisCommandBase() {}

        RedisCommandBase(const char* s)
        {
            addPart(s);
        }

        RedisCommandBase(const std::basic_string<CharT>& s)
        {
            addPart(s);
        }

        RedisCommandBase(const std::vector<std::string>& parts)
            : _parts(parts) { }

        RedisCommandBase(const RedisCommandBase<CharT>& from)
            : _parts(from._parts) { }

        const std::string& operator[](size_t i) const
        {
            return _parts[i];
        }

        size_t size() const
        {
            return _parts.size();
        }

        RedisCommandBase<CharT>& operator=(const std::vector<std::string>& parts)
        {
            _parts = parts;
            return *this;
        }

        RedisCommandBase<CharT>& operator=(const RedisCommandBase<CharT>& from)
        {
            _parts = from._parts;
            return *this;
        }

        RedisCommandBase<CharT>& operator<<(const std::basic_string<CharT>& s)
        {
            addPart(s);
            return *this;
        }

        RedisCommandBase<CharT>& operator<<(const char* s)
        {
            addPart(s);
            return *this;
        }

        RedisCommandBase<CharT>& operator<<(const std::vector<std::basic_string<CharT> >& ss)
        {
            _parts.reserve(_parts.size() + ss.size());

            for (size_t i = 0; i < ss.size(); ++i)
            {
                addPart(ss[i]);
            }

            return *this;
        }

        template<class T> RedisCommandBase<CharT>& operator<<(const T& v)
        {
            addPart(boost::lexical_cast<std::basic_string<CharT> >(v));
            return *this;
        }
    };

    template<typename CharT>
    class RedisBase : public RedisConst<CharT>
    {
        mutable redisContext* _context;

        std::string _host;
        int _port;

        RedisBase(const RedisBase<CharT>&);
        RedisBase<CharT>& operator=(const RedisBase<CharT>&);

        void connect() const
        {
            if (_context == 0)
            {
                _context = ::redisConnect(_host.c_str(), _port);

                if (_context->err)
                {
                    RedisException e(_context->errstr);

                    ::redisFree(_context);
                    _context = 0;

                    throw e;
                }
            }
        }

    public:
        typedef RedisCommandBase<CharT> Command;
        typedef RedisResult<RedisReplyBase, CharT> Reply;
        typedef RedisResult<RedisElementBase, CharT> Element;

        RedisBase(const std::string& host, int port = 6379)
            : _context(0), _host(host), _port(port) { }

        virtual ~RedisBase()
        {
            if (_context != 0)
            {
                ::redisFree(_context);

                _context = 0;
            }
        }

        const std::string& host() const { return _host; }
        int port() const { return _port; }

        Reply endCommand() const
        {
            redisReply* r;

            if (::redisGetReply(_context, reinterpret_cast<void**>(&r)) != REDIS_OK)
            {
                RedisException e(_context->errstr);

                ::redisFree(_context);
                _context = 0;

                throw e;
            }

            return Reply(r);
        }

        void beginInfo() const
        {
            connect();

            ::redisAppendCommand(_context, "INFO");
        }

        std::map<std::basic_string<CharT>, std::basic_string<CharT> > info() const
        {
            beginInfo();

            std::map<std::basic_string<CharT>, std::basic_string<CharT> > info;
            std::basic_string<CharT> lines = endCommand();

            size_t i = 0;
            size_t j = lines.find(RedisConst<CharT>::InfoCrLf);

            while (i != std::basic_string<CharT>::npos)
            {
                std::basic_string<CharT> line = lines.substr(i, j == std::basic_string<CharT>::npos ? j : j - i);
                i = j == std::basic_string<CharT>::npos ? j : j + RedisConst<CharT>::InfoCrLf.size();
                j = lines.find(RedisConst<CharT>::InfoCrLf, i);

                size_t p = line.find(RedisConst<CharT>::InfoSeparator);
                
                if (p != std::basic_string<CharT>::npos &&
                    p < (line.size() - 1))
                {
                    info[line.substr(0, p)] = line.substr(p + 1, std::basic_string<CharT>::npos);
                }
            }

            return info;
        }

        void beginPing() const
        {
            connect();

            ::redisAppendCommand(_context, "PING");
        }

        std::basic_string<CharT> ping() const
        {
            beginPing();
            return endCommand().getStatus();
        }

        void beginSelect(int database) const
        {
            connect();

            ::redisAppendCommand(_context, "SELECT %d", database);
        }

        void select(int database) const
        {
            beginSelect(database);
            endCommand();
        }

        void beginGet(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("GET") << key);
        }

        std::basic_string<CharT> get(const std::basic_string<CharT>& key) const
        {
            beginGet(key);
            return endCommand();
        }

        void beginMget(const std::vector<std::basic_string<CharT> >& keys) const
        {
            connect();
            beginCommand(Command("MGET") << keys);
        }

        Reply mget(const std::vector<std::basic_string<CharT> >& keys) const
        {
            beginMget(keys);
            return endCommand();
        }

        void beginExists(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("EXISTS") << key);
        }

        bool exists(const std::basic_string<CharT>& key) const
        {
            beginExists(key);
            return(((boost::int64_t)endCommand()) != 0);
        }

        void beginSet(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            connect();
            beginCommand(Command("SET") << key << value);
        }

        void set(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            beginSet(key, value);
            endCommand();
        }

        void beginSetnx(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            connect();
            beginCommand(Command("SETNX") << key << value);
        }

        boost::int64_t setnx(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            beginSetnx(key, value);
            return endCommand();
        }

        void beginIncr(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("INCR") << key);
        }

        boost::int64_t incr(const std::basic_string<CharT>& key) const
        {
            beginIncr(key);
            return endCommand();
        }

        void beginKeys(const std::basic_string<CharT>& pattern) const
        {
            connect();
            beginCommand(Command("KEYS") << pattern);
        }

        Reply keys(const std::basic_string<CharT>& pattern) const
        {
            beginKeys(pattern);
            return endCommand();
        }

        void beginDel(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("DEL") << key);
        }

        boost::int64_t del(const std::basic_string<CharT>& key) const
        {
            beginDel(key);
            return endCommand();
        }

        void beginDel(const std::vector<std::basic_string<CharT> >& keys) const
        {
            connect();
            beginCommand(Command("DEL") << keys);
        }

        boost::int64_t del(const std::vector<std::basic_string<CharT> >& keys) const
        {
            beginDel(keys);
            return endCommand();
        }

        void beginLpush(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            connect();
            beginCommand(Command("LPUSH") << key << value);
        }

        void lpush(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            beginLpush(key, value);
            endCommand();
        }

        void beginLpop(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("LPOP") << key);
        }

        std::basic_string<CharT> lpop(const std::basic_string<CharT>& key) const
        {
            beginLpop(key);
            return endCommand();
        }

        void beginRpush(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            connect();
            beginCommand(Command("RPUSH") << key << value);
        }

        void rpush(const std::basic_string<CharT>& key, const std::basic_string<CharT>& value) const
        {
            beginRpush(key, value);
            endCommand();
        }

        void beginRpop(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("RPOP") << key);
        }

        std::basic_string<CharT> rpop(const std::basic_string<CharT>& key) const
        {
            beginRpop(key);
            return endCommand();
        }

        void beginLindex(const std::basic_string<CharT>& key, boost::int64_t index) const
        {
            connect();
            beginCommand(Command("LINDEX") << key << index);
        }

        std::basic_string<CharT> lindex(const std::basic_string<CharT>& key, boost::int64_t index) const
        {
            beginLindex(key, index);
            return endCommand();
        }

        void beginLrange(const std::basic_string<CharT>& key, boost::int64_t start, boost::int64_t end) const
        {
            connect();
            beginCommand(Command("LRANGE") << key << start << end);
        }

        Reply lrange(const std::basic_string<CharT>& key, boost::int64_t start, boost::int64_t end) const
        {
            beginLrange(key, start, end);
            return endCommand();
        }

        void beginLlen(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("LLEN") << key);
        }

        boost::int64_t llen(const std::basic_string<CharT>& key) const
        {
            beginLlen(key);
            return endCommand();
        }


        void beginHget(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field) const
        {
            connect();
            beginCommand(Command("HGET") << key << field);
        }

        std::basic_string<CharT> hget(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field) const
        {
            beginHget(key, field);
            return endCommand();
        }

        void beginHdel(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field) const
        {
            connect();
            beginCommand(Command("HDEL") << key << field);
        }

        boost::int64_t hdel(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field) const
        {
            beginHdel(key, field);
            return endCommand();
        }

        void beginHset(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field, const std::basic_string<CharT>& value) const
        {
            connect();
            beginCommand(Command("HSET") << key << field << value);
        }

        boost::int64_t hset(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field, const std::basic_string<CharT>& value) const
        {
            beginHset(key, field, value);
            return endCommand();
        }

        void beginHsetnx(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field, const std::basic_string<CharT>& value) const
        {
            connect();
            beginCommand(Command("HSETNX") << key << field << value);
        }

        boost::int64_t hsetnx(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field, const std::basic_string<CharT>& value) const
        {
            beginHsetnx(key, field, value);
            return endCommand();
        }

        void beginHincrby(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field, boost::int64_t value) const
        {
            connect();
            beginCommand(Command("HINCRBY") << key << field << value);
        }

        boost::int64_t hincrby(const std::basic_string<CharT>& key, const std::basic_string<CharT>& field, boost::int64_t value) const
        {
            beginHincrby(key, field, value);
            return endCommand();
        }

        void beginHgetall(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("HGETALL") << key);
        }

        Reply hgetall(const std::basic_string<CharT>& key) const
        {
            beginHgetall(key);
            return endCommand();
        }

        void beginSadd(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("SADD") << key << member);
        }

        boost::int64_t sadd(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginSadd(key, member);
            return endCommand();
        }

        void beginSismember(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("SISMEMBER") << key << member);
        }

        bool sismember(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginSismember(key, member);
            return ((boost::int64_t)endCommand() == 1);
        }

        void beginSrem(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("SREM") << key << member);
        }

        boost::int64_t srem(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginSrem(key, member);
            return endCommand();
        }

        void beginSmembers(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("SMEMBERS") << key);
        }

        Reply smembers(const std::basic_string<CharT>& key) const
        {
            beginSmembers(key);
            return endCommand();
        }

        void beginSdiff(const std::basic_string<CharT>& key, const std::vector<std::basic_string<CharT> >& diffKeys) const
        {
            connect();
            beginCommand(Command("SDIFF") << key << diffKeys);
        }

        Reply sdiff(const std::basic_string<CharT>& key, const std::vector<std::basic_string<CharT> >& diffKeys) const
        {
            beginSdiff(key, diffKeys);
            return endCommand();
        }

        void beginSdiff(const std::basic_string<CharT>& key, const std::basic_string<CharT>& diffKey) const
        {
            connect();
            beginCommand(Command("SDIFF") << key << diffKey);
        }

        Reply sdiff(const std::basic_string<CharT>& key, const std::basic_string<CharT>& diffKey) const
        {
            beginSdiff(key, diffKey);
            return endCommand();
        }

        void beginSunion(const std::vector<std::basic_string<CharT> >& keys) const
        {
            connect();
            beginCommand(Command("SUINION") << keys);
        }

        Reply sunion(const std::vector<std::basic_string<CharT> >& keys) const
        {
            beginSunion(keys);
            return endCommand();
        }

        void beginSunion(const std::basic_string<CharT>& key0, const std::basic_string<CharT>& key1) const
        {
            connect();
            beginCommand(Command("SUNION") << key0 << key1);
        }

        Reply sunion(const std::basic_string<CharT>& key0, const std::basic_string<CharT>& key1) const
        {
            beginSunion(key0, key1);
            return endCommand();
        }

        void beginScard(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("SCARD") << key);
        }

        boost::int64_t scard(const std::basic_string<CharT>& key) const
        {
            beginScard(key);
            return endCommand();
        }

        void beginZadd(const std::basic_string<CharT>& key, double score, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("ZADD") << key << score << member);
        }

        boost::int64_t zadd(const std::basic_string<CharT>& key, double score, const std::basic_string<CharT>& member) const
        {
            beginZadd(key, score, member);
            return endCommand();
        }

        void beginZrem(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("ZREM") << key << member);
        }

        boost::int64_t zrem(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginZrem(key, member);
            return endCommand();
        }

        void beginZrank(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("ZRANK") << key << member);
        }

        boost::optional<boost::int64_t> zrank(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginZrank(key, member);
            return endCommand();
        }

        void beginZrevrank(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            connect();
            beginCommand(Command("ZREVRANK") << key << member);
        }

        boost::optional<boost::int64_t> zrevrank(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginZrevrank(key, member);
            return endCommand();
        }

        void beginZrange(const std::basic_string<CharT>& key, boost::int64_t start, boost::int64_t end) const
        {
            connect();
            beginCommand(Command("ZRANGE") << key << start << end);
        }

        Reply zrange(const std::basic_string<CharT>& key, boost::int64_t start, boost::int64_t end) const
        {
            beginZrange(key, start, end);
            return endCommand();
        }

        void beginZrevrange(const std::basic_string<CharT>& key, boost::int64_t start, boost::int64_t end) const
        {
            connect();
            beginCommand(Command("ZREVRANGE") << key << start << end);
        }

        Reply zrevrange(const std::basic_string<CharT>& key, boost::int64_t start, boost::int64_t end) const
        {
            beginZrevrange(key, start, end);
            return endCommand();
        }

        void beginZrangebyscore(const std::basic_string<CharT>& key,const std::basic_string<CharT>& min, const std::basic_string<CharT>& max) const
        {
            connect();
            beginCommand(Command("ZRANGEBYSCORE") << key << min << max);
        }

        Reply zrangebyscore(const std::basic_string<CharT>& key, const std::basic_string<CharT>& min, const std::basic_string<CharT>& max) const
        {
            beginZrangebyscore(key, min, max);
            return endCommand();
        }

        void beginZrevrangebyscore(const std::basic_string<CharT>& key, const std::basic_string<CharT>& max, const std::basic_string<CharT>& min) const
        {
            connect();
            beginCommand(Command("ZREVRANGEBYSCORE") << key << max << min);
        }

        Reply zrevrangebyscore(const std::basic_string<CharT>& key, const std::basic_string<CharT>& max, const std::basic_string<CharT>& min) const
        {
            beginZrevrangebyscore(key, max, min);
            return endCommand();
        }

        void beginZcard(const std::basic_string<CharT>& key) const
        {
            connect();
            beginCommand(Command("ZCARD") << key);
        }

        boost::int64_t zcard(const std::basic_string<CharT>& key) const
        {
            beginZcard()(key);
            return endCommand();
        }

        void beginCommand(const Command& command) const
        {
            connect();

            const char* argv[command.size()];
            size_t argvlen[command.size()];

            for (size_t i = 0; i < command.size(); ++i)
            {
                argv[i] = command[i].data();
                argvlen[i] = command[i].size();
            }

            ::redisAppendCommandArgv(_context, command.size(), argv, argvlen);
        }

        Reply doCommand(const Command& command) const
        {
            beginCommand(command);
            return endCommand();
        }

        void doPipeline(const std::vector<Command>& commands) const
        {
            for (size_t i = 0; i < commands.size(); ++i)
            {
                beginCommand(commands[i]);
            }

            for (size_t i = 0; i < commands.size(); ++i)
            {
                endCommand();
            }
        }

        void doPipeline(const std::vector<Command>& commands, std::vector<Reply>& replies) const
        {
            for (size_t i = 0; i < commands.size(); ++i)
            {
                beginCommand(commands[i]);
            }

            replies.reserve(commands.size());

            for (size_t i = 0; i < commands.size(); ++i)
            {
                replies.push_back(endCommand());
            }
        }

        void beginWatch(const std::vector<std::basic_string<CharT> >& keys) const
        {
            beginCommand(Command("WATCH") << keys);
        }

        void watch(const std::vector<std::basic_string<CharT> >& keys) const
        {
            beginWatch(keys);
            endCommand();
        }

        void watch(const std::basic_string<CharT>& key) const
        {
            std::vector<std::basic_string<CharT> > keys;
            keys.push_back(key);
            watch(keys);
        }

        void beginUnwatch() const
        {
            beginCommand(Command("UNWATCH"));
        }

        void unwatch() const
        {
            beginUnwatch();
            endCommand();
        }

        Reply doTransaction(const std::vector<Command>& commands) const
        {
            beginCommand(Command("MULTI"));

            for (size_t i = 0; i < commands.size(); ++i)
            {
                beginCommand(commands[i]);
            }

            beginCommand(Command("EXEC"));
            endCommand();

            for (size_t i = 0; i < commands.size(); ++i)
            {
                endCommand();
            }

            return endCommand();
        }
    };

    typedef RedisBase<char> Redis;
    typedef RedisBase<wchar_t> wRedis;

    template<>
    inline void RedisEncoding<char>::decode(const char* data, size_t size,
                                            std::basic_string<char>& string)
    {
        string.assign(data, size);
    }

    template<>
    inline void RedisEncoding<char>::decode(const std::string& data,
                                            std::basic_string<char>& string)
    {
        string=data;
    }

    template<>
    inline void RedisEncoding<char>::encode(const std::basic_string<char>& string,
                                            std::string& data)
    {
        data.resize(0);
        data.append(string.begin(), string.end());
    }

    template<>
    inline void RedisEncoding<char>::encode(const std::basic_string<char>& string,
                                            std::ostream& data)
    {
        data << string;
    }


}

#endif // HIREDISPP_H
