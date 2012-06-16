#include "Timer.h"

#include <iostream> 


Timer::Timer()
   : startMark(),
     endMark(),
     eventName()
{}

void
Timer::start()
{
   gettimeofday(&startMark, NULL);
}

void
Timer::start(std::string name)
{
   eventName = name;
   gettimeofday(&startMark, NULL);  
}

void
Timer::reset()
{
   stop();
   start();
}

void
Timer::reset(std::string name)
{
   stop();
   start(name);
}



void
Timer::stop()
{
   gettimeofday(&endMark, NULL);
   eval(1);
}


void
Timer::stop(int iterations)
{
   gettimeofday(&endMark, NULL);
   eval(iterations);
}

void
Timer::stopPlot()
{
   gettimeofday(&endMark, NULL);
   eval(0);
}

void
Timer::eval(int iterations)
{
   long mtime, seconds, useconds;

   seconds  = endMark.tv_sec  - startMark.tv_sec;
   useconds = endMark.tv_usec - startMark.tv_usec;

   mtime = ((seconds) * 1000 + useconds/1000.0) + 0.5;
    
   // Just plot mtime e.g. for using output for Gnuplot
   if(iterations==0)
   {
      std::cout << mtime;
   }


   if (eventName=="")
   {
       if(iterations==1)
       { 
           std::cout << "Elapsed time: " << mtime << "ms" << std::endl;
       }
       else if(iterations!=0)
       {
           std::cout << "Elapsed time: " << mtime/iterations 
           << "ms per iteration" << std::endl;
       }
   }
   else
   {
       if(iterations==1)
       {
           std::cout << "Elapsed time for " << eventName << ": " << mtime  
           << "ms" << std::endl;
       }
       else if(iterations!=0)
       {

           std::cout << "Elapsed time for " << eventName << ": "
           << mtime/iterations << "ms per iteration" << std::endl;
       }  
   }
   
   eventName = "";    
}

