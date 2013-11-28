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

class ConsumerServlet(topic:String, groupId:String, zookeeperUrl:String) extends HttpServlet
{
  val consumer = Consumer.create(getConsumerConfig())
  val logger = Logger.getLogger("kafka.rest.consumer")
  val stream  = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

  override def doGet(request:HttpServletRequest, response:HttpServletResponse)
  {
    logger.warn("Started logging")
    for (item <- stream) {
      var key = new String(item.key)
      var message = new String(item.message)
      logger.warn("Hi, I have just got a message: %s %s".format(key, message))
      response.setContentType("application/json")
      response.setStatus(HttpServletResponse.SC_OK)
      response.getWriter().println("{\"" +key+ "\": \""+  message  +"\"}")
      return
    }
  }

  def getConsumerConfig():ConsumerConfig = {
    val props = new Properties();
    props.put("zookeeper.connect", zookeeperUrl)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "largest")
    return new ConsumerConfig(props)
  }
}
