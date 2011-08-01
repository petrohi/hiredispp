#ifndef _HiredisppAsync_H_
#define _HiredisppAsync_H_

#include <hiredis/adapters/libev.h>
#include <memory>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#define HIREDISPP_DEBUG

namespace hiredispp
{
    class RedisConnectionAsync
    {
    public:
        RedisConnectionAsync(const std::string& host, int port)
            : _ac(NULL), _host(host), _port(port), _reconnect(false)
        {}

        template<typename HandlerC, typename HandlerD>
        void connect(HandlerC handlerC, HandlerD handlerD)
        {
            _onConnected = createOnHandler(handlerC);
            _onDisconnected = createOnHandler(handlerD);
            asyncConnect();
        }
        
        void disconnect()
        {
            if (_ac) {
                redisAsyncDisconnect(_ac);
                _ac=NULL;
            }
        }

        template<typename CharT, typename ExecHandler>
        void execAsyncCommand(const RedisCommandBase<CharT> & cmd, ExecHandler handler)
        {
            if (_ac==NULL || _ac->c.flags & (REDIS_DISCONNECTING | REDIS_FREEING))
                throw RedisException("Can't execute a command, disconnecting or freeing");

            const int sz=cmd.size();
            const char* argv[sz];
            size_t argvlen[sz];
            
            for (size_t i = 0; i < sz; ++i) {
                argv[i] = cmd[i].data();
                argvlen[i] = cmd[i].size();
            }
            
            Handler<ExecHandler> *hand=new Handler<ExecHandler>(handler);
            int result = 
                ::redisAsyncCommandArgv(_ac, Handler<ExecHandler>::callback, hand,
                                        sz, argv, argvlen);

            if (result == REDIS_ERR) {
                delete hand;
                throw RedisException("Can't execute a command, REDIS ERROR");
            }
        }

    private:
        typedef RedisConnectionAsync ThisType;

        class BaseOnHandler
        {
        public:
            virtual void operator() (boost::shared_ptr<RedisException> &ex)=0;
            virtual ~BaseOnHandler() {}
        };

        template<typename Handler>
        class OnHandler : public BaseOnHandler
        {
            Handler _handler;
        public:
            OnHandler(Handler handler) : _handler(handler) {}
            virtual void operator() (boost::shared_ptr<RedisException> &ex) {
#ifdef HIREDISPP_DEBUG
                std::cout<<"virtual operator("<<(ex.get() ? "EXCEPTION" : "NULL")<<")"<<std::endl;
#endif
                _handler(ex);
            }
        };
        
        template<typename HandlerT>
        typename std::auto_ptr< BaseOnHandler > createOnHandler(HandlerT handler)
        {
            return std::auto_ptr< BaseOnHandler >(new OnHandler<HandlerT>(handler));
        }

        template<typename Callback>
        class Handler // : public enable_shared_from_this<Handler <Callback> >
        {
        public:
            Handler(Callback c) : _c(c) {}

            static void callback(redisAsyncContext *c, void *reply, void *privdata)
            {
                (static_cast< Handler<Callback> * > (privdata)) -> operator() (c,reply);
            }

            void operator() (redisAsyncContext *c, void *reply)
            {
                if (reply) {
                    // not a Redis::Reply, to avoid re-release of reply
                    Redis::Element replyPtr(static_cast<redisReply*>(reply));
                    _c(*static_cast<ThisType*>(c->data), &replyPtr);
                }
                else {
                    _c(*static_cast<ThisType*>(c->data), static_cast<Redis::Element*>(NULL));
                }
                delete(this);
            }

        private:
            Callback _c;
        };
        
        void onConnected(int status)
        {
            boost::shared_ptr<RedisException> ex;
            if (status!=REDIS_OK) {
                ex.reset(new RedisException((_ac && _ac->errstr) ? _ac->errstr : "REDIS_ERR"));
                asyncClose(); // we started ev_read/write, we must stop it.
                _ac=NULL;
            }
            _onConnected->operator()(ex);
        }
        
        void onDisconnected(int status)
        {
            boost::shared_ptr<RedisException> ex;
            if (status!=REDIS_OK) {
                ex.reset(new RedisException((_ac && _ac->errstr) ? _ac->errstr : "REDIS_ERR"));
                _ac=NULL;
            }
            _onDisconnected->operator()(ex);
        }

        static void connected(const redisAsyncContext *ac, int status)
        {
            std::cout<<"static::connected"<<std::endl;
            if (ac && ac->data) {
                ((RedisConnectionAsync*)(ac->data))->onConnected(status);
            }
        }
        
        static void disconnected(const redisAsyncContext *ac, int status)
        {
            std::cout<<"static::disconnected"<<std::endl;
            if (ac && ac->data) {
                ((RedisConnectionAsync*)(ac->data))->onDisconnected(status);
            }
        }

        std::string        _host;
        uint16_t           _port;
        bool               _reconnect;
        redisAsyncContext* _ac;

        std::auto_ptr<BaseOnHandler>   _onConnected;
        std::auto_ptr<BaseOnHandler>   _onDisconnected;

        int asyncConnect()
        {
#ifdef HIREDISPP_DEBUG
            std::cout<<"asyncConnect()"<<std::endl;
#endif
            assert(_ac==NULL); 

            _ac = redisAsyncConnect(_host.c_str(), _port);
            _ac->data = (void*)this;

            if (_ac->err) {
                throw RedisException((std::string)"RedisAsyncConnect: "+_ac->errstr);
            }
            if (redisAsyncSetConnectCallback(_ac, &connected)!=REDIS_OK ||
                redisAsyncSetDisconnectCallback(_ac, &disconnected)!=REDIS_OK) {
                throw RedisException("RedisAsyncConnect: Can't register callbacks");
            }

            if (redisLibevAttach(EV_DEFAULT, _ac)!=REDIS_OK) {
                throw RedisException("redisLibevAttach: nothing should be attached when something is already attached");
            }

            // actually start io proccess
            ev_io_start(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->rev));
            ev_io_start(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->wev));
        }

        void asyncClose()
        {
            if (_ac) {
                ev_io_stop(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->rev));
                ev_io_stop(EV_DEFAULT, &((((redisLibevEvents*)(_ac->ev.data)))->wev));
                // redisLibevCleanup(_ac->_adapter_data);
                // TODO: should we close it or not???
                // close(_ac->c.fd);
            }
        }
    };

}
#endif
