package kafka.rest

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool

import kafka.utils._

import org.apache.log4j.Logger

object RestServer
{
  val logger = Logger.getLogger("kafka.rest.server")

  def main(args:Array[String]){
    try{
      var servlet = Configurator.getServlet(args)
      var server = new Server(8080)

      // This is to serialize access to endpoints
      var threadPool = new QueuedThreadPool(10)
      server.setThreadPool(threadPool)
      var context = new ServletContextHandler(ServletContextHandler.SESSIONS)

      context.setContextPath("/")
      server.setHandler(context)
 
      context.addServlet(new ServletHolder(servlet),"/*")

      server.start()
      server.join()

    } catch {
      case e: Exception => {
        logger.error(e.toString)
        System.exit(1)
      }
    }

  }
}
