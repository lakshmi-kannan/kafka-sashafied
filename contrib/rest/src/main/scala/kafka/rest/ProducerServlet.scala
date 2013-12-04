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
import scala.collection.mutable._

import com.mongodb.util.JSON

import org.bson.BSON
import org.bson.BSONObject
import org.bson.BasicBSONDecoder
import org.bson.BasicBSONEncoder
import org.bson.types.BasicBSONList

class ProducerServlet(properties:Properties) extends HttpServlet
{
  val producer = new Producer[String, Array[Byte]](new ProducerConfig(properties))
  val logger = Logger.getLogger("kafka.rest.producer")

  def asList(name: String, o:AnyRef):BasicBSONList = {
    try{
      return o.asInstanceOf[BasicBSONList]
    }
    catch {
      case e: Exception => {
        throw new Exception("Expected list in %s".format(name))
      }
    }
  }

  def asObject(name:String, o:AnyRef):BSONObject = {
    try{
      return o.asInstanceOf[BSONObject]
    }
    catch {
      case e: Exception => {
        throw new Exception("Expected object in %s".format(name))
      }
    }
  }

  def asString(name:String, o:AnyRef):String = {
    try{
      return o.asInstanceOf[String]
    }
    catch {
      case e: Exception => {
        throw new Exception("Expected object in %s".format(name))
      }
    }
  }

  def toKeyedMessage(topic:String, o:BSONObject):KeyedMessage[String, Array[Byte]] = {
    var key:String = asString("'key' property", o.get("key"))
    var value = asObject("'value' property", o.get("value"))
    if(value == null) {
      throw new Exception("Expected 'value' property in message")
    }
    return new KeyedMessage[String, Array[Byte]](topic, key, new BasicBSONEncoder().encode(value))
  }

  def getObject(request:HttpServletRequest):MutableList[KeyedMessage[String, Array[Byte]]] = {
    var topic = getTopic(request)

    val obj = request.getContentType() match {
      case "application/json" => getObjectFromJson(request)
      case "application/bson" => getObjectFromBson(request)
      case _ => throw new Exception("Unsupported content type: %s".format(request.getContentType()))
    }
    var messagesI = obj.get("messages")
    if(messagesI == null) {
      throw new Exception("Expected 'messages' list")
    }
    var messages = asList("'messages' parameter", messagesI)
    var list = new MutableList[KeyedMessage[String, Array[Byte]]]()

    for (messageI <- messages) {
      var message = asObject("message", messageI)
      list += toKeyedMessage(topic, message)
      logger.info("Got something: %s".format(list))
    }
    list
  }

  def getObjectFromBson(request:HttpServletRequest):BSONObject = {
    return new BasicBSONDecoder().readObject(request.getInputStream())
  }

  def getObjectFromJson(request:HttpServletRequest):BSONObject = {
    var body = new StringBuilder
    var reader = request.getReader()
    var buffer = new Array[Char](4096)
    var len:Int = 0

    while ({len = reader.read(buffer, 0, buffer.length); len != -1}) {
      body.appendAll(buffer, 0, len);
    }
    return JSON.parse(body.toString()).asInstanceOf[BSONObject]
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
    var messages = getObject(request)

    val data = new KeyedMessage[String, Array[Byte]](topic, "key", new Array[Byte](1))
    producer.send(messages:_*)

    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter().println("{\"hello\": \"world post\"}")
  }
}
