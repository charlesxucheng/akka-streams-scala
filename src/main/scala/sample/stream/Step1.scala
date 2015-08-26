package sample.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.FanInShape2
import akka.stream.FanInShape
import akka.stream.FanInShape.Name
import akka.stream.scaladsl.FlexiMerge
import akka.stream.FanInShape.Init
import akka.stream.Attributes
import scala.collection.mutable.HashMap
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.FlowGraph

object Step1 {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher

    implicit val materializer = ActorMaterializer()

    val mv: List[(String, BigDecimal)] = List(("C100", 100), ("C101", 200))
    val adj: List[(String, BigDecimal)] = List(("C100", 10), ("C102", -10))

    val mvSrc = Source(() => mv.iterator)
    val adjSrc = Source(() => adj.iterator)

    val g = FlowGraph.closed(Sink.foreach(println)) { implicit b =>
      o =>
        import FlowGraph.Implicits._
        val cvStage = b.add(new ComputeCvStage)
        mvSrc ~> cvStage.mv
        adjSrc ~> cvStage.adj
        cvStage.out ~> o.inlet
    }

    val result = g.run().onComplete { _ => system.shutdown() }

    // could also use .runWith(Sink.foreach(println)) instead of .runForeach(println) above
    // as it is a shorthand for the same thing. Sinks may be constructed elsewhere and plugged
    // in like this. Note also that foreach returns a future (in either form) which may be
    // used to attach lifecycle events to, like here with the onComplete.
  }
}

class MvAdjMergeShape[A](_init: Init[A] = Name("MvAdjMerge"))
    extends FanInShape[A](_init) {
  val mv = newInlet[A]("mv")
  val adj = newInlet[A]("adj")
  protected override def construct(i: Init[A]) = new MvAdjMergeShape(i)
}

class ComputeCvStage extends FlexiMerge[(String, BigDecimal), MvAdjMergeShape[(String, BigDecimal)]](
  new MvAdjMergeShape, Attributes.name("ImportantWithBackups")) {
  import akka.stream.scaladsl.FlexiMerge._
  override def createMergeLogic(p: PortT) = new MergeLogic[(String, BigDecimal)] {

    var mvMap: HashMap[String, BigDecimal] = HashMap.empty[String, BigDecimal]
    var adjMap: HashMap[String, BigDecimal] = HashMap.empty[String, BigDecimal]

    val readElement: State[(String, BigDecimal)] = State[(String, BigDecimal)](ReadAny(p.mv, p.adj)) { (ctx, input, element) =>
      input match {
        case p.mv => {
          println("Got an MV " + element)
          mvMap.put(element._1, element._2)
          ctx.emit((element._1, element._2 + adjMap.getOrElse(element._1, 0)))
        }
        case p.adj => {
          println("Got an Adj" + element)
          adjMap.put(element._1, element._2)
          ctx.emit((element._1, element._2 + mvMap.getOrElse(element._1, 0)))
        }
        case _ => println("Error!");
      }
      SameState
    }

    override def initialState = readElement

  }
}