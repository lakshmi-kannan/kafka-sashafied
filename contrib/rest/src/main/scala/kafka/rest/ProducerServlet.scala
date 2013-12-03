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

import kafka.producer._
import kafka.message._
import kafka.serializer._

class ProducerServlet(brokerList:String) extends HttpServlet
{
  val producer = new Producer[String, String](getProducerConfig())
  val logger = Logger.getLogger("kafka.rest.producer")

  def getBody(request:HttpServletRequest):String = {
    var body = new StringBuilder
    var reader = request.getReader()
    var buffer = new Array[Char](4096)
    var len:Int = 0

    while ({len = reader.read(buffer, 0, buffer.length); len != -1}) {
      body.appendAll(buffer, 0, len);
    }
    return body.toString()
  }

  def getTopic(request:HttpServletRequest):String = {
    var segments = request.getRequestURI().split("/")
    if (segments.length != 3 || segments(1) != "topics") {
      throw new Exception("Please provide topic /topics/<topic> to post to: %s".format(segments(1)) )
    } 
    return segments(2)
  }

  override def doPost(request:HttpServletRequest, response:HttpServletResponse)
  {
    var topic = getTopic(request)
    var body = getBody(request)
    if(body.length == 0) {
      throw new Exception("Provide request body")
    }

    logger.warn("Received post request to topic: %s, body: '%s'".format(topic, body))

    val data = new KeyedMessage[String, String](topic, "key", body)
    producer.send(data)

    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter().println("{\"hello\": \"world post\"}")
  }

  def getProducerConfig():ProducerConfig = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    return new ProducerConfig(props)
  }

}
