// Simple server
import java.net.ServerSocket
import java.io.{BufferedReader, InputStreamReader, PrintStream}
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder


object Server extends App {
  val BUCKET_NAME = ""
  val FILE_NAME = ""

  val AWS_ACCESS_KEY_ID = ""
  val AWS_SECRET_ACCESS_KEY = ""
  val AWS_SESSION_TOKEN = ""

  def randomStringGen(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

  def getS3Data: BufferedReader = {
    try {
      val awsCredentials = new BasicSessionCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
      val awsS3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
        .build()

      val obj = awsS3Client.getObject(BUCKET_NAME, FILE_NAME)
      new BufferedReader(new InputStreamReader(obj.getObjectContent))
    } catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString); return null
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString); return null
    }

  }

  val server = new ServerSocket(9999)
  while (true) {
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream)

    // Read data from S3
    val br = getS3Data
    var line = br.readLine
    while (line != null) {
      out.println(line)
      out.flush()
      line = br.readLine
    }
    br.close()

    s.close()
  }
}
