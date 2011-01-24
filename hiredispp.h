/*
 * hiredispp.h
 */

#ifndef HIREDISPP_H
#define HIREDISPP_H

#include <string.h>
#include <string>
#include <vector>
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
		static void decode(const char* data, size_t size, std::basic_string<CharT>& string);
		static void encode(const std::basic_string<CharT>& string, std::string& data);
	};

	class RedisException : public std::exception
	{
		std::string _what;

	public:
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
	};

	template<class T, typename CharT>
	class RedisResult : private RedisEncoding<CharT>, public T
	{
		void checkError() const
		{
			if (isError())
			{
				RedisException e(std::string(T::get()->str, T::get()->len));

				throw e;
			}
		}

		std::basic_string<CharT> getString() const
		{
			std::basic_string<CharT> s;
			decode(T::get()->str, T::get()->len, s);
			return s;
		}

	public:
		RedisResult(redisReply* r)
			: T(r) { }

		bool isError() const
		{
			return (T::get()->type == REDIS_REPLY_ERROR);
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
	class RedisCommandBase : private RedisEncoding<CharT>, public std::vector<std::string>
	{
		void append(const std::basic_string<CharT>& s)
		{
			std::string data;
			encode(s, data);
			push_back(data);
		}

		void append(const char* s)
		{
			std::string data(s, s + ::strlen(s));
			push_back(data);
		}

	public:
		RedisCommandBase(const char* s)
		{
			append(s);
		}

		RedisCommandBase(const std::basic_string<CharT>& s)
		{
			append(s);
		}

		RedisCommandBase(const std::vector<std::vector<char> >& from)
			: std::vector<std::vector<char> >(from) { }

		RedisCommandBase<CharT>& operator=(const std::vector<std::vector<char> >& from)
		{
			*this = from;
			return *this;
		}

		RedisCommandBase<CharT>& operator<<(const std::basic_string<CharT>& s)
		{
			append(s);
			return *this;
		}

		RedisCommandBase<CharT>& operator<<(const char* s)
		{
			append(s);
			return *this;
		}

		RedisCommandBase<CharT>& operator<<(const std::vector<std::basic_string<CharT> >& ss)
		{
			for (size_t i = 0; i < ss.size(); ++i)
			{
				append(ss[i]);
			}

			return *this;
		}

		template<class T> RedisCommandBase<CharT>& operator<<(const T& v)
		{
			append(boost::lexical_cast<std::basic_string<CharT> >(v));
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

		Reply execute(const Command& command) const
		{
			beginCommand(command);
			return endCommand();
		}

		void execute(const std::vector<Command>& commands, std::vector<Reply>& replies) const
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

	typedef RedisBase<char> Redis;
	typedef RedisBase<wchar_t> wRedis;

	template<>
	inline void RedisEncoding<char>::decode(const char* data, size_t size, std::basic_string<char>& string)
	{
		string.assign(data, size);
	}

	template<>
	inline void RedisEncoding<char>::encode(const std::basic_string<char>& string, std::string& data)
	{
		data.append(string.begin(), string.end());
	}

}

#endif // HIREDISPP_H
