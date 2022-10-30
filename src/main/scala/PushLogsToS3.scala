import HelperUtils.{CreateLogger, ObtainConfigReference}
import com.amazonaws.AmazonServiceException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.slf4j.Logger

import java.io.{BufferedWriter, File, FileWriter}
import java.util
import java.util.regex.Pattern
import scala.collection.mutable
import scala.io.Source


object PushLogsToS3 {

  def uploadLogs(): Unit =
    val config = ObtainConfigReference("s3") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    val clientRegion = Regions.US_EAST_2
    val bucketName = config.getString("s3.bucketName")
    val logsDirectory = new File(config.getString("s3.logsDirectory"))
    val filesList = logsDirectory.listFiles()
    val s3 = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build()

    val hashMap: mutable.Map[String, util.ArrayList[String]] = new mutable.HashMap()

    val pattern = Pattern.compile(config.getString("s3.logFileNamePattern"))
    filesList.foreach(file => {
      val matcher = pattern.matcher(file.getName)
      matcher.matches()
      val date = matcher.group(1)
      val (first, last) = getFirstAndLastDate(file.getAbsolutePath)
      if(hashMap.contains(date)) {
        val data = hashMap(date)
        data.add(s"$first|$last|${config.getString("s3.s3LogKeyPrefix")}${file.getName}")
      } else {
        val newData = new util.ArrayList[String]()
        newData.add(s"$first|$last|${config.getString("s3.s3LogKeyPrefix")}${file.getName}")
        hashMap.put(date, newData)
      }

    })

    val hashTableFileName = config.getString("s3.hashTableFileName")
    val file = new File(hashTableFileName)
    if(file.exists()) {
      file.delete()
    }
    val bw = new BufferedWriter(new FileWriter(file))

    hashMap.keys.foreach(key => {
      bw.write(key)
      bw.write(config.getString("s3.hashTableKeyValueSeparator"))
      val data = hashMap(key)
      data.forEach(item => {
        bw.write(item)
        bw.write(config.getString("s3.hashTableEachFileDataSeparator"))
      })
      bw.write("\n")
    })
    bw.close()

    try {
      filesList.map(file => s3.putObject(bucketName, s"${config.getString("s3.s3LogKeyPrefix")}${file.getName}", new File(file.getAbsolutePath)))
      s3.putObject(bucketName, s"${config.getString("s3.hashTableS3Directory")}$hashTableFileName", file)
    } catch {
      case e: AmazonServiceException =>
        println(e.getErrorMessage)
        System.exit(1)
    }


  def getFirstAndLastDate(path: String): (String, String) =
    val lines = Source.fromFile(path).getLines()
    val firstLine = if(lines.hasNext) Some(lines.next) else None
    val lastLine = lines.foldLeft(Option.empty[String]) { case (_, line) => Some(line) }
    (firstLine.get.split(' ')(0), lastLine.get.split(' ')(0))
}
