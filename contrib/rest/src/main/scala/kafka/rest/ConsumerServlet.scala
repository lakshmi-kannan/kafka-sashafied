package kafka.rest;

import scala.collection.JavaConversions._

import java.io.IOException
import java.util.Properties
import java.util.HashMap

import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.apache.log4j.Logger;

import kafka.consumer._
import kafka.message._
import kafka.serializer._

import org.bson.BasicBSONDecoder
import com.mongodb.util.JSON

class ConsumerServlet(topic:String, properties: Properties) extends HttpServlet
{
  val consumer = Consumer.create(new ConsumerConfig(properties))
  val logger = Logger.getLogger("kafka.rest.consumer")
  val stream  = consumer.createMessageStreamsByFilter(
    new Whitelist(topic), 1, new StringDecoder(), new DefaultDecoder()).get(0)

  override def doPost(request:HttpServletRequest, response:HttpServletResponse)
  {
    logger.info("Started commiting offsets")
    this.synchronized {
      consumer.commitOffsets
    }
    logger.info("Done commiting offsets")
    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter().println("{\"commited\": \"OK\"}")
  }

  override def doGet(request:HttpServletRequest, response:HttpServletResponse)
  {
    this.synchronized {
      logger.info("Initiated get")
      for (item <- stream) {
        var key = new String(if (item.key == null) "" else item.key)
        var message = new BasicBSONDecoder().readObject(item.message)

        logger.warn("Hi, I have just got a message: %s %s".format(key, message))
        response.setContentType("application/json")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().println(JSON.serialize(message))
        return
      }
    }
  }
}
