/* hiredispp.h -- a C++ wrapper for hiredis.
 */

#ifndef HIREDISPP_H
#define HIREDISPP_H

#include <hiredis/hiredis.h>
#include <string>
#include <stdexcept>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>

namespace hiredispp
{
	const std::string Nil = "**NIL**";

	template<class T> class RedisResult;

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

	typedef RedisResult<RedisElementBase> RedisElement;

	template<class T> class RedisResult : public T
	{
	public:
		RedisResult(redisReply* r)
			: T(r) { }

		std::string getStatus() const
		{
			if (T::get()->type != REDIS_REPLY_STATUS)
			{
				throw std::runtime_error("Invalid reply type");
			}

			return std::string(T::get()->str, T::get()->len);
		}

		operator std::string() const
		{
			if (
					T::get()->type != REDIS_REPLY_STRING &&
					T::get()->type != REDIS_REPLY_NIL)
			{
				throw std::runtime_error("Invalid reply type");
			}

			if (T::get()->type == REDIS_REPLY_NIL)
			{
				return Nil;
			}

			return std::string(T::get()->str, T::get()->len);
		}

		operator boost::int64_t() const
		{
			if (T::get()->type != REDIS_REPLY_INTEGER)
			{
				throw std::runtime_error("Invalid reply type");
			}

			return T::get()->integer;
		}

		size_t size() const
		{
			if (T::get()->type != REDIS_REPLY_ARRAY)
			{
				throw std::runtime_error("Invalid reply type");
			}

			return T::get()->elements;
		}

		RedisElement operator[](size_t i) const
		{
			if (T::get()->type != REDIS_REPLY_ARRAY)
			{
				throw std::runtime_error("Invalid reply type");
			}

			if (i >= T::get()->elements)
			{
				throw std::runtime_error("Out of range");
			}

			return RedisElement(T::get()->element[i]);
		}

		template <class V>
		void toValue(V& v)
		{
			v = boost::lexical_cast<V>((std::string)(*this));
		}

		template <class V>
		void toVector(std::vector<V>& v)
		{
			for (size_t i = 0; i < size(); ++i)
			{
				v.push_back(boost::lexical_cast<V>((std::string)(*this)[i]));
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

	typedef RedisResult<RedisReplyBase> RedisReply;

	class RedisException : public std::exception
	{
		std::string _what;

	public:
		RedisException(const std::string& what)
		{
			_what = what;
		}

		virtual ~RedisException() throw() { }

		virtual const char* what() const throw()
		{
			return _what.c_str();
		}
	};

	class RedisCommand : public std::vector<std::string>
	{
	public:
		RedisCommand(const std::string& s)
		{
			push_back(s);
		}

		RedisCommand(const std::vector<std::string>& from)
			: std::vector<std::string>(from) { }

		RedisCommand& operator=(const std::vector<std::string>& from)
		{
			*this = from;
			return *this;
		}

		RedisCommand& operator<<(const std::string& s)
		{
			push_back(s);
			return *this;
		}

		RedisCommand& operator<<(const std::vector<std::string>& ss)
		{
			for (size_t i = 0; i < ss.size(); ++i)
			{
				push_back(ss[i]);
			}

			return *this;
		}

		template<class T> RedisCommand& operator<<(const T& v)
		{
			push_back(boost::lexical_cast<std::string>(v));
			return *this;
		}
	};

	class Redis
	{
		mutable redisContext* _context;

		std::string _host;
		int _port;

		Redis(const Redis&);
		Redis& operator=(const Redis&);

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

		RedisReply makeReply(redisReply* r) const
		{
			if (r->type == REDIS_REPLY_ERROR)
			{
				RedisException e(std::string(r->str, r->len));
				::freeReplyObject(r);

				throw e;
			}

			return RedisReply(r);
		}

	public:
		Redis(const std::string& host, int port = 6379)
			: _context(0), _host(host), _port(port) { }

		virtual ~Redis()
		{
			if (_context != 0)
			{
				::redisFree(_context);

				_context = 0;
			}
		}

		RedisReply endCommand() const
		{
			redisReply* r;

			if (::redisGetReply(_context, reinterpret_cast<void**>(&r)) != REDIS_OK)
			{
				RedisException e(_context->errstr);

				::redisFree(_context);
				_context = 0;

				throw e;
			}

			return makeReply(r);
		}

		void beginPing() const
		{
			connect();

			::redisAppendCommand(_context, "PING");
		}

		std::string ping() const
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

		void beginGet(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "GET %b", key.c_str(), key.size());
		}

		std::string get(const std::string& key) const
		{
			beginGet(key);
			return endCommand();
		}

		void beginSet(const std::string& key, const std::string& value) const
		{
			connect();

			::redisAppendCommand(_context, "SET %b %b", key.c_str(), key.size(), value.c_str(), value.size());
		}

		void set(const std::string& key, const std::string& value) const
		{
			beginSet(key, value);
			endCommand();
		}

		void beginSetnx(const std::string& key, const std::string& value) const
		{
			connect();

			::redisAppendCommand(_context, "SETNX %b %b", key.c_str(), key.size(), value.c_str(), value.size());
		}

		boost::int64_t setnx(const std::string& key, const std::string& value) const
		{
			beginSetnx(key, value);
			return endCommand();
		}

		void beginIncr(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "INCR %b", key.c_str(), key.size());
		}

		boost::int64_t incr(const std::string& key) const
		{
			beginIncr(key);
			return endCommand();
		}

		void beginKeys(const std::string& pattern) const
		{
			connect();

			::redisAppendCommand(_context, "KEYS %b", pattern.c_str(), pattern.size());
		}

		RedisReply keys(const std::string& pattern) const
		{
			beginKeys(pattern);
			return endCommand();
		}

		void beginDel(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "DEL %b", key.c_str(), key.size());
		}

		boost::int64_t del(const std::string& key) const
		{
			beginDel(key);
			return endCommand();
		}

		void beginDel(const std::vector<std::string>& keys) const
		{
			connect();

			beginCommand(RedisCommand("DEL") << keys);
		}

		boost::int64_t del(const std::vector<std::string>& keys) const
		{
			beginDel(keys);
			return endCommand();
		}

		void beginHget(const std::string& key, const std::string& field) const
		{
			connect();

			::redisAppendCommand(_context, "HGET %b %b", key.c_str(), key.size(), field.c_str(), field.size());
		}

		std::string hget(const std::string& key, const std::string& field) const
		{
			beginHget(key, field);
			return endCommand();
		}

		void beginHset(const std::string& key, const std::string& field, const std::string& value) const
		{
			connect();

			::redisAppendCommand(_context, "HSET %b %b %b",
					key.c_str(), key.size(), field.c_str(), field.size(), value.c_str(), value.size());
		}

		boost::int64_t hset(const std::string& key, const std::string& field, const std::string& value) const
		{
			beginHset(key, field, value);
			return endCommand();
		}

		void beginHincrby(const std::string& key, const std::string& field, boost::int64_t value) const
		{
			connect();

			::redisAppendCommand(_context, "HINCRBY %b %b %d", key.c_str(), key.size(), field.c_str(), field.size(), value);
		}

		boost::int64_t hincrby(const std::string& key, const std::string& field, boost::int64_t value) const
		{
			beginHincrby(key, field, value);
			return endCommand();
		}

		void beginHgetall(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "HGETALL %b", key.c_str(), key.size());
		}

		RedisReply hgetall(const std::string& key) const
		{
			beginHgetall(key);
			return endCommand();
		}

		void beginSadd(const std::string& key, const std::string& member) const
		{
			connect();

			::redisAppendCommand(_context, "SADD %b %b", key.c_str(), key.size(), member.c_str(), member.size());
		}

		boost::int64_t sadd(const std::string& key, const std::string& member) const
		{
			beginSadd(key, member);
			return endCommand();

		}

		void beginSmembers(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "SMEMBERS %b", key.c_str(), key.size());
		}

		RedisReply smembers(const std::string& key) const
		{
			beginSmembers(key);
			return endCommand();
		}

		void beginScard(const std::string& key) const
		{
			connect();

			::redisAppendCommand(_context, "SCARD %b", key.c_str(), key.size());
		}

		boost::int64_t scard(const std::string& key) const
		{
			beginSmembers(key);
			return endCommand();
		}

		void beginCommand(const RedisCommand& command) const
		{
			connect();

			const char* argv[command.size()];
			size_t argvlen[command.size()];

			for (size_t i = 0; i < command.size(); ++i)
			{
				argv[i] = command[i].c_str();
				argvlen[i] = command[i].size();
			}

			::redisAppendCommandArgv(_context, command.size(), argv, argvlen);
		}

		RedisReply execute(const RedisCommand& command) const
		{
			beginCommand(command);
			return endCommand();
		}

		void execute(const std::vector<RedisCommand>& commands, std::vector<RedisReply>& replies) const
		{
			for (size_t i = 0; i < commands.size(); ++i)
			{
				beginCommand(commands[i]);
			}

			for (size_t i = 0; i < commands.size(); ++i)
			{
				replies.push_back(endCommand());
			}
		}
	};
}

#endif // HIREDISPP_H
