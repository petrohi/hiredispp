//
// g++ async_example.cpp -o async_example -I.. -lboost_program_options -lhiredis -lev
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <string>
#include <iostream>
#include <hiredis/async.h>

#include <hiredispp/hiredispp.h>
#include <hiredispp/hiredispp_async.h>

#include <boost/bind.hpp>
#include <boost/random.hpp>
#include <boost/program_options.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics.hpp>

using namespace std;
using namespace hiredispp;
using namespace boost::posix_time;
using namespace boost::accumulators;

namespace po = boost::program_options;

class Main
{
    typedef accumulator_set<double, stats <tag::median(with_p_square_quantile),
                                           tag::skewness, // add mean, moment<2>, moment<3>
                                           tag::count, tag::min, tag::max, tag::mean 
                                           > > AccuType;

public:
    Main(const string& host, int port, int total, int batch, const string& key, bool is_set, int vsize) :
        _host(host), _port(port), _connected(false),
        _ac(_host, _port), _counter(0), _done(0), _total(total),
        _batch(batch), _key(key), _set(is_set), _vsize(vsize),
        _dist(1, (vsize >0 ? vsize : 1)), _rand(_rng, _dist)
    {
        connect();
        _start=microsec_clock::universal_time();
        _cstart=_start;
    }

    ~Main()
    {
        if (_connected) {
            _ac.disconnect();
        }
        boost::posix_time::ptime stop=microsec_clock::universal_time();
        cout << "FINAL:" << endl;
        print_stats(cout, _start, stop, _lat) << endl;
    }

    ostream& print_stats(ostream& out,
                         boost::posix_time::ptime start,
                         boost::posix_time::ptime stop,
                         const AccuType& acc) const
    {
        long long int per = time_period(start, stop).length().total_microseconds();
        return out
            << extract_result< tag::count >(acc) << " "
            << extract_result< tag::min >(acc) << " "
            << mean(acc) << " "
            << extract_result< tag::max >(acc) << " "
            << extract_result< tag::median >(acc) << " "
            << sqrt(extract_result< tag::moment<2> > (acc)) << " "
            << extract_result< tag::skewness > (acc)
            << "\ttime: "<< per
            << "\tRPS "  << 1e6 * extract_result< tag::count >(acc) / per;
    }

    void connect()
    {
        _ac.connect(boost::bind(&Main::onConnected, this, _1),
                    boost::bind(&Main::onDisconnected, this, _1));
    }

    void setDone(RedisConnectionAsync& ac, Redis::Element *reply, int id, boost::posix_time::ptime start)
    {
        boost::posix_time::ptime stop=microsec_clock::universal_time();
        try {
            if (reply) {
                ++_done;
                reply->checkError();
            }
            else {
                throw RedisException(std::string("disconnected"));
            }
            
            _lat(time_period(start, stop).length().total_microseconds());
            _cur(time_period(start, stop).length().total_microseconds());

            if (_done==_counter && (_batch!=1 || (_done % (_total/10)==0))) {
                cout << "exec.done "<<_counter
                     <<(reply ? " REPLY " : " NULL ");
                print_stats(cout, _cstart, stop, _cur) << endl;
                _cstart = stop;
                _cur=AccuType();
            }
            
            if (_done==_counter) {
                executeNext();
            }
        }
        catch (const RedisException& ex) {
            cout<<"Main::execCmd exception "<<ex.what()<<endl;
            if (_connected) 
                _ac.disconnect();
        }
    }

    void onConnected(boost::shared_ptr<RedisException> &ex)
    {
        cout << "Main::onConnected: " 
             << (ex ? ex->what() : "OK")
             << endl;

        if (ex==NULL) {
            _connected=true;
            executeNext();
        }
    }

    void onDisconnected(boost::shared_ptr<RedisException> &ex)
    {
        cout << "Main::onDisconnected: " 
             << (ex ? ex->what() : "OK") 
             << endl;
        
        _connected=false;
    }

    void executeNext()
    {
        if (_counter<_total) {
            try {
                for (int i=0; i<_batch && _counter<_total; ++i) {
                    boost::posix_time::ptime start=microsec_clock::universal_time();
                    RedisCommandBase<char> cmd;
                    if (_set) {
                        cmd<<"set"
                           <<(_key+boost::lexical_cast<string> (_counter));
                        if (_vsize==0) {
                            cmd<<((string)"myvalue" + boost::lexical_cast<string> (_counter));
                        }
                        else {
                            cmd<<string(_rand(),'v');
                        }
                    }
                    else {
                        cmd<<"get"
                           <<(_key+boost::lexical_cast<string> (_counter));
                    }
                    _ac.execAsyncCommand(cmd, boost::bind(&Main::setDone, this, _1, _2, _counter, start));
                    ++_counter;
                }
            }
            catch (const hiredispp::RedisException& ex) {
                cout<<ex.what()<<endl;
                _ac.disconnect();
            }
        }
        else {
            if (_done==_total) {
                _ac.disconnect();
            }
        }
    }

    string _host;
    int    _port;
    bool   _connected;
    int    _counter;
    int    _done;

    boost::posix_time::ptime _start;
    boost::posix_time::ptime _cstart;

    const string _key;
    const int _batch;
    const int _total;
    const int _vsize;
    const bool _set;

    boost::mt19937        _rng;
    boost::uniform_int<> _dist;
    boost::variate_generator<boost::mt19937&, boost::uniform_int<> > _rand;

    RedisConnectionAsync _ac;

    AccuType _cur;
    AccuType _lat;
};

namespace std {
    std::ostream& operator<< (std::ostream& out, const std::pair<std::string, std::string> &p) {
        return out << p.first << ":" << p.second;
    }
}

int main(int argc, char** argv)
{
    string host;
    string key;
    int    port;
    int    count;
    int    batch;
    int    vsize;
    bool   is_set;

    po::options_description desc("options");

    desc.add_options()
        ("help", "produce this help message")
        ("host", po::value<string>(&host)->default_value("localhost"), "host")
        ("port", po::value<int>(&port)->default_value(6379), "port")
        ("count", po::value<int>(&count)->default_value(1000), "number of requests")
        ("batch", po::value<int>(&batch)->default_value(100), "batch size")
        ("is_set", po::value<bool>(&is_set)->default_value(true), "set/get command")
        ("key", po::value<string>(&key)->default_value("mykey"), "key prefix")
        ("value_size", po::value<int>(&vsize)->default_value(0), "maximum value size, 0 - fixed.")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    
    if (vm.count("help")) {
        cout << desc << endl;
        return 0;
    }

    signal(SIGPIPE, SIG_IGN);
    ev_default_loop(0);
    Main main(host, port, count, batch, key, is_set, vsize);
    ev_loop(EV_DEFAULT, 0);
    return 0;
}
