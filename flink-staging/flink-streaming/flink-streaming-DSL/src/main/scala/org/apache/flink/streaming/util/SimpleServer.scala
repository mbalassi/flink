package org.apache.flink.streaming.util

import java.net.{Socket, ServerSocket}

import scala.io.Source

class SimpleServer (port: Int)extends Runnable{
  val serverSocket = new ServerSocket(port)
  
  def run() ={
    while (true){
      val socket = serverSocket.accept()
      for (line <- Source.fromFile("./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt").getLines){
        socket.getOutputStream.write(line.getBytes())
        socket.getOutputStream.write("\n".getBytes)
        Thread.sleep(1000)
      }
      socket.getOutputStream.close()
      
    }
  }
 
}

object NetServer{
  def main(args: Array[String]) {
    new SimpleServer(2015).run()
  }
  
}


// sudo lsof -t -i:2015
// kill


//test: nc localhost 2015
