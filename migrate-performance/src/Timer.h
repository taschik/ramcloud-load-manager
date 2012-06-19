#include <string>
#include <sys/time.h>
#include <unistd.h>

class Timer{
  public:
    Timer();
    void start();
    void start(std::string name);
    void stop();
    void stop(int iterations);
    void stopPlot();    
    void reset();
    void reset(std::string name);
    
  private:
    struct timeval startMark, endMark;
    std::string eventName;
    void eval(int iterations); 
};

