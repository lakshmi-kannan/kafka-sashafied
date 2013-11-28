package kafka.rest

import java.util.Random

import javax.servlet.http.HttpServlet

import joptsimple._
import kafka.utils._

import org.apache.log4j.Logger

object Configurator {
  val logger = Logger.getLogger("kafka.rest.consumer")

  def getServlet(args:Array[String]):HttpServlet = {
    if(args.length < 1) {
      throw new Exception("Provide first parameter as 'consumer' or 'producer'")
    }

    var endpoint = args(0)
    if(endpoint == "consumer") {
      return getConsumerServlet(args)
    } else if (endpoint == "producer") {
      return getProducerServlet(args)
    } else {
      throw new Exception("Provide first parameter as 'consumer' or 'producer'")
    }
  }

  def getConsumerServlet(args:Array[String]):HttpServlet = {
    val parser = new OptionParser

    val topic = parser.accepts("topic", "The topic to consume from")
            .withRequiredArg
            .describedAs("topic")
            .ofType(classOf[String])

    val group = parser.accepts("group", "The group id of this consumer")
            .withRequiredArg
            .describedAs("gid")
            .defaultsTo("rest-" + new Random().nextInt(100000))
            .ofType(classOf[String])

    val zookeeper = parser.accepts(
      "zookeeper", "Comma separated of zookeeper nodes")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val options: OptionSet = tryParse(parser, args)
    CommandLineUtils.checkRequiredArgs(parser, options, topic, group, zookeeper)

    return new ConsumerServlet(
      options.valueOf(topic).toString,
      options.valueOf(group).toString,
      options.valueOf(zookeeper).toString)
  }

  def getProducerServlet(args:Array[String]):HttpServlet = {
    val parser = new OptionParser

    val brokers = parser.accepts(
      "broker-list", "Comma separated of kafka nodes")
      .withRequiredArg
      .describedAs("brokers")
      .ofType(classOf[String])

    val options: OptionSet = tryParse(parser, args)
    CommandLineUtils.checkRequiredArgs(parser, options, brokers)
    return new ProducerServlet(options.valueOf(brokers).toString)
  }

  def tryParse(parser: OptionParser, args: Array[String]) = {
    try {
      parser.parse(args : _*)
    } catch {
      case e: OptionException => {
        Utils.croak(e.getMessage)
        null
      }
    }
  }
}
